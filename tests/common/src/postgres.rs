//! A throwaway Postgres schema per test.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use distributed::PostgresRepository;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;

use crate::env as env_support;

static NEXT_SCHEMA_ID: AtomicU64 = AtomicU64::new(1);

/// A fresh schema of `DATABASE_URL`, dropped (with everything in it) when this
/// value is dropped — including when the test panics.
///
/// Keep it alive as long as any repository or pool made from it: those
/// connections resolve tables through `search_path`, so they stop working once
/// the schema is gone. Fixtures return it next to what they build
/// (`(schema, repo)`); bind it to a named variable, not `_`, which drops at once.
pub struct PostgresTestSchema {
    database_url: String,
    schema_name: String,
}

impl PostgresTestSchema {
    pub async fn create_from_env(prefix: &str, what: &str) -> Option<Self> {
        let database_url = env_support::require_service("DATABASE_URL", what)?;

        Some(
            Self::create(&database_url, prefix)
                .await
                .expect("Postgres test schema should create"),
        )
    }

    pub async fn create(database_url: &str, prefix: &str) -> Result<Self, sqlx::Error> {
        let schema_name = unique_schema_name(prefix);
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await?;
        sqlx::query(audited_schema_sql(format!(
            "CREATE SCHEMA {}",
            quote_identifier(&schema_name)
        )))
        .execute(&admin_pool)
        .await?;
        admin_pool.close().await;

        Ok(Self {
            database_url: database_url.to_string(),
            schema_name,
        })
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub async fn repository(&self) -> PostgresRepository {
        let repo = self.repository_unmigrated().await;
        repo.migrate()
            .await
            .expect("Postgres test repository should migrate");
        repo
    }

    pub async fn repository_unmigrated(&self) -> PostgresRepository {
        PostgresRepository::new(
            PgPoolOptions::new()
                .max_connections(crate::POOL_CONNECTIONS)
                .after_connect({
                    let schema_name = self.schema_name.clone();
                    move |connection, _metadata| {
                        let search_path =
                            format!("SET search_path TO {}", quote_identifier(&schema_name));
                        Box::pin(async move {
                            connection.execute(audited_schema_sql(search_path)).await?;
                            Ok(())
                        })
                    }
                })
                .connect(&self.database_url)
                .await
                .expect("Postgres test repository should connect"),
        )
    }
}

impl Drop for PostgresTestSchema {
    /// `Drop` cannot await, and the test's runtime may be the current thread,
    /// so the drop runs on a short-lived thread with its own runtime. A
    /// connection that still holds a lock in the schema (a transaction left
    /// open by a panic) makes the drop give up after `lock_timeout` instead of
    /// hanging the test; the schema is then left behind, as before.
    fn drop(&mut self) {
        let database_url = self.database_url.clone();
        let drop_schema = format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(&self.schema_name)
        );
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())?;
            runtime.block_on(async move {
                let pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&database_url)
                    .await
                    .map_err(|error| error.to_string())?;
                let mut connection = pool.acquire().await.map_err(|error| error.to_string())?;
                connection
                    .execute("SET lock_timeout = '5s'")
                    .await
                    .map_err(|error| error.to_string())?;
                connection
                    .execute(audited_schema_sql(drop_schema))
                    .await
                    .map_err(|error| error.to_string())?;
                drop(connection);
                pool.close().await;
                Ok::<_, String>(())
            })
        })
        .join();
        let error = match dropped {
            Ok(Ok(())) => return,
            Ok(Err(error)) => error,
            Err(_) => "the drop thread panicked".to_owned(),
        };
        eprintln!("left test schema {} behind: {error}", self.schema_name);
    }
}

fn unique_schema_name(prefix: &str) -> String {
    let prefix = sanitize_prefix(prefix);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    let sequence = NEXT_SCHEMA_ID.fetch_add(1, Ordering::Relaxed);
    format!("sr_{prefix}_{nanos}_{sequence}")
}

fn sanitize_prefix(prefix: &str) -> String {
    let mut sanitized = prefix
        .chars()
        .filter_map(|character| {
            if character.is_ascii_alphanumeric() {
                Some(character.to_ascii_lowercase())
            } else if character == '_' {
                Some(character)
            } else {
                None
            }
        })
        .take(18)
        .collect::<String>();

    if sanitized.is_empty() {
        sanitized.push_str("test");
    }

    sanitized
}

fn quote_identifier(identifier: &str) -> String {
    let escaped = identifier.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

fn audited_schema_sql(statement: String) -> sqlx::AssertSqlSafe<String> {
    // Test schema names are generated locally, sanitized, and quoted as identifiers
    // before they are interpolated into schema-management statements.
    sqlx::AssertSqlSafe(statement)
}
