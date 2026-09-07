use crate::graphql::engine::GraphqlPool;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

const MAX_TABLES: usize = 128;
// The cached envelope includes projection evidence as well as model data.
// Version all persisted evidence dependencies, including no-op/empty commits.
const PROOF_TABLES: &[&str] = &[
    "projection_partitions",
    "projection_generations",
    "projection_source_capabilities",
    "projection_input_identities",
    "projection_input_cursors",
    "projection_input_receipts",
    "projection_table_ownership_fences",
    "projection_causal_tables",
    "projection_registered_models",
    "projection_model_ownership",
    "projection_records",
    "projection_observations",
    "projection_failures",
    "projection_changes",
];

/// Installed transactional dependency coverage for one origin database. Create
/// after application migrations, before enabling gateway caching. Every covered
/// table receives write triggers; unsupported/uncovered tables remain ineligible.
#[derive(Clone)]
pub struct GatewayVersionStore {
    namespace: Arc<str>,
    tables: Arc<BTreeSet<String>>,
    counters: Arc<Counters>,
    proof_tables: Arc<BTreeSet<String>>,
    public_policies: BTreeMap<(String, Option<String>), u64>,
}
#[derive(Default)]
struct Counters {
    validations: AtomicU64,
    result_executions: AtomicU64,
}
/// Origin work counters; result executions exclude dependency validation SQL.
#[derive(Clone, Copy, Debug)]
pub struct GatewayOriginMetrics {
    /// Authenticated validation requests reaching the configured origin store.
    pub validations: u64,
    /// Actual compiled result SQL executions using the protocol snapshot path.
    pub result_executions: u64,
}
#[derive(Clone, Debug, Serialize)]
pub(crate) struct DependencyVersion {
    epoch: String,
    version: String,
}
pub(crate) type VersionVector = BTreeMap<String, DependencyVersion>;

impl GatewayVersionStore {
    /// Explicitly permit bounded content age for one exact ordinary operation
    /// (all its variable values). Subject isolation and fresh origin admission
    /// still apply. Omit this declaration for current/private reads.
    pub fn public_snapshot(
        mut self,
        document: impl Into<String>,
        operation: Option<String>,
        max_age_seconds: u64,
    ) -> Result<Self, String> {
        let document = document.into();
        if self.public_policies.len() >= 512
            || !(1..=86400).contains(&max_age_seconds)
            || crate::gateway::graphql::operation_kind(&document, operation.as_deref())
                != Ok(crate::gateway::graphql::OperationKind::Query)
        {
            return Err("invalid public snapshot policy".into());
        }
        self.public_policies
            .insert((document, operation), max_age_seconds);
        Ok(self)
    }
    pub(crate) fn policy(
        &self,
        document: &str,
        operation: Option<&str>,
    ) -> crate::gateway::delivery::SnapshotPolicy {
        self.public_policies
            .get(&(document.to_owned(), operation.map(str::to_owned)))
            .map_or(
                crate::gateway::delivery::SnapshotPolicy::Current,
                |seconds| crate::gateway::delivery::SnapshotPolicy::Public {
                    max_age_seconds: *seconds,
                },
            )
    }
    /// Snapshot origin work counters without resetting concurrent observations.
    pub fn metrics(&self) -> GatewayOriginMetrics {
        GatewayOriginMetrics {
            validations: self.counters.validations.load(Ordering::Relaxed),
            result_executions: self.counters.result_executions.load(Ordering::Relaxed),
        }
    }
    pub(crate) fn record_validation(&self) {
        self.counters.validations.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_result(&self) {
        self.counters
            .result_executions
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Install additive version metadata and triggers in one transaction. Data
    /// migration/cleanup is never implicit. Namespace is a configured app ID.
    pub async fn install(
        pool: &GraphqlPool,
        namespace: &str,
        tables: impl IntoIterator<Item = String>,
    ) -> Result<Self, String> {
        let tables: BTreeSet<_> = tables.into_iter().collect();
        if namespace.is_empty()
            || namespace.len() > 128
            || tables.is_empty()
            || tables.len() > MAX_TABLES
            || tables
                .iter()
                .any(|t| t.is_empty() || t.len() > 128 || t.chars().any(char::is_control))
        {
            return Err("invalid dependency version inventory".into());
        }
        let epoch = uuid::Uuid::now_v7().to_string();
        let mut store = Self {
            namespace: namespace.into(),
            tables: Arc::new(tables),
            counters: Arc::default(),
            proof_tables: Arc::default(),
            public_policies: BTreeMap::new(),
        };
        match pool {
            #[cfg(feature = "sqlite")]
            GraphqlPool::Sqlite(pool) => {
                let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
                let existing: Vec<String> =
                    sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type='table'")
                        .fetch_all(&mut *tx)
                        .await
                        .map_err(|e| e.to_string())?;
                store.proof_tables = Arc::new(
                    existing
                        .into_iter()
                        .filter(|table| PROOF_TABLES.contains(&table.as_str()))
                        .collect(),
                );
                for sql in store.install_sql(&epoch, false) {
                    sqlx::query(sqlx::AssertSqlSafe(sql))
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                tx.commit().await.map_err(|e| e.to_string())?;
            }
            #[cfg(feature = "postgres")]
            GraphqlPool::Postgres(pool) => {
                let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
                super::super::execute::ensure_primary_backend(&mut tx, true).await?;
                let existing: Vec<String> = sqlx::query_scalar(
                    "SELECT tablename::text FROM pg_tables WHERE schemaname=current_schema()",
                )
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| e.to_string())?;
                store.proof_tables = Arc::new(
                    existing
                        .into_iter()
                        .filter(|table| PROOF_TABLES.contains(&table.as_str()))
                        .collect(),
                );
                for sql in store.install_sql(&epoch, true) {
                    sqlx::query(sqlx::AssertSqlSafe(sql))
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                tx.commit().await.map_err(|e| e.to_string())?;
            }
            #[allow(unreachable_patterns)]
            _ => return Err("dependency versions require a SQL adapter".into()),
        }
        Ok(store)
    }
    fn install_sql(&self, epoch: &str, postgres: bool) -> Vec<String> {
        let mut sql =
            vec![
                include_str!("../../../migrations/postgres/0006_gateway_dependency_versions.sql")
                    .to_owned(),
            ];
        for table in self.tables.union(&self.proof_tables) {
            let ns = literal(&self.namespace);
            let table_literal = literal(table);
            let name = self.trigger_name(table);
            sql.push(format!("INSERT INTO distributed_gateway_versions(namespace, table_name, epoch, version) VALUES ({ns},{table_literal},{},0) ON CONFLICT(namespace,table_name) DO UPDATE SET epoch=excluded.epoch,version=0",literal(epoch)));
            let update = format!("UPDATE distributed_gateway_versions SET version=version+1 WHERE namespace={ns} AND table_name={table_literal};");
            if postgres {
                sql.push(format!(
                    "CREATE OR REPLACE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS {}",
                    ident(&name),
                    literal(&format!("BEGIN {update} RETURN NULL; END"))
                ));
                sql.push(format!(
                    "DROP TRIGGER IF EXISTS {} ON {}",
                    ident(&name),
                    ident(table)
                ));
                sql.push(format!("CREATE TRIGGER {} AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON {} FOR EACH STATEMENT EXECUTE FUNCTION {}()",ident(&name),ident(table),ident(&name)));
            } else {
                for event in ["INSERT", "UPDATE", "DELETE"] {
                    sql.push(format!(
                        "CREATE TRIGGER IF NOT EXISTS {} AFTER {event} ON {} BEGIN {update} END",
                        ident(&format!("{name}_{event}")),
                        ident(table)
                    ));
                }
            }
        }
        sql
    }
    fn trigger_name(&self, table: &str) -> String {
        let digest =
            Sha256::digest(format!("gateway-version-v1:{}:{table}", self.namespace).as_bytes());
        format!("dg_v_{digest:x}")[..55].into()
    }
    pub(crate) fn envelope_coverage(&self) -> bool {
        self.proof_tables.len() == PROOF_TABLES.len()
    }
    pub(crate) fn covers(&self, tables: &[String]) -> bool {
        !tables.is_empty()
            && tables.len() <= MAX_TABLES
            && tables.iter().all(|t| self.tables.contains(t))
    }
    /// Read current validators on the authoritative primary only. Result fills
    /// use the connection methods inside their data snapshot instead.
    pub(crate) async fn current(
        &self,
        pool: &GraphqlPool,
        tables: &[String],
    ) -> Result<VersionVector, String> {
        match pool {
            #[cfg(feature = "sqlite")]
            GraphqlPool::Sqlite(pool) => {
                let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
                let result = self.sqlite(&mut tx, tables).await?;
                tx.commit().await.map_err(|e| e.to_string())?;
                Ok(result)
            }
            #[cfg(feature = "postgres")]
            GraphqlPool::Postgres(pool) => {
                let mut tx = pool.begin().await.map_err(|e| e.to_string())?;
                sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| e.to_string())?;
                super::super::execute::ensure_primary_backend(&mut tx, true).await?;
                let result = self.postgres(&mut tx, tables).await?;
                tx.commit().await.map_err(|e| e.to_string())?;
                Ok(result)
            }
            #[allow(unreachable_patterns)]
            _ => Err("dependency versions require a SQL adapter".into()),
        }
    }
    /// Activate a new explicit epoch after a rebuild or writer-coverage change.
    /// Call in a controlled migration window; old validators become unusable.
    pub async fn rotate_epoch(&self, pool: &GraphqlPool) -> Result<(), String> {
        let epoch = uuid::Uuid::now_v7().to_string();
        let sql = format!(
            "UPDATE distributed_gateway_versions SET epoch={},version=0 WHERE namespace={}",
            literal(&epoch),
            literal(&self.namespace)
        );
        match pool {
            #[cfg(feature = "sqlite")]
            GraphqlPool::Sqlite(pool) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .execute(pool)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            #[cfg(feature = "postgres")]
            GraphqlPool::Postgres(pool) => {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .execute(pool)
                    .await
                    .map_err(|e| e.to_string())?;
            }
            #[allow(unreachable_patterns)]
            _ => return Err("dependency versions require a SQL adapter".into()),
        }
        Ok(())
    }
}

// SQLx database implementations keep trigger coverage checks and version reads
// in the caller's actual serving snapshot. Missing/disabled hooks fail closed.
macro_rules! version_reader {
    ($method:ident, $connection:ty, $coverage:expr) => {
        impl GatewayVersionStore {
            pub(crate) async fn $method(&self, connection: &mut $connection, tables: &[String]) -> Result<VersionVector, String> {
                if !self.covers(tables) { return Err("query dependency coverage is incomplete".into()); }
                let selected: BTreeSet<_> = tables.iter().chain(self.proof_tables.iter()).collect();
                let queries = selected.iter().map(|table| {
                    let coverage_sql: String = ($coverage)(self, table);
                    format!("SELECT table_name, epoch, version, ({coverage_sql}) AS covered FROM distributed_gateway_versions WHERE namespace={} AND table_name={}",literal(&self.namespace),literal(table))
                }).collect::<Vec<_>>().join(" UNION ALL ");
                let rows: Vec<(String,String,i64,i64)> = sqlx::query_as(sqlx::AssertSqlSafe(queries)).fetch_all(&mut *connection).await.map_err(|e| e.to_string())?;
                if rows.len() != selected.len() { return Err("dependency version coverage is unavailable".into()); }
                let mut result = BTreeMap::new();
                for (table, epoch, version, covered) in rows {
                    if covered != 1 || version < 0 { return Err("dependency invalidation coverage is unavailable".into()); }
                    result.insert(table, DependencyVersion { epoch, version: version.to_string() });
                }
                Ok(result)
            }
        }
    }
}
#[cfg(feature = "sqlite")]
version_reader!(
    sqlite,
    sqlx::SqliteConnection,
    |store: &GatewayVersionStore, table: &str| {
        let name = store.trigger_name(table);
        format!("SELECT CASE WHEN COUNT(*)=3 THEN 1 ELSE 0 END FROM sqlite_master WHERE type='trigger' AND tbl_name={} AND name IN ({},{},{})",literal(table),literal(&format!("{name}_INSERT")),literal(&format!("{name}_UPDATE")),literal(&format!("{name}_DELETE")))
    }
);
#[cfg(feature = "postgres")]
version_reader!(
    postgres,
    sqlx::PgConnection,
    |store: &GatewayVersionStore, table: &str| {
        format!("SELECT COUNT(*)::bigint FROM pg_trigger WHERE tgrelid={}::regclass AND tgname={} AND tgenabled IN ('O','A')",literal(&ident(table)),literal(&store.trigger_name(table)))
    }
);
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
fn ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    // The same behavioral matrix runs against both actual SQL adapters. It
    // includes a concurrent data/version snapshot and supported external SQL.
    macro_rules! exercise {
        ($pool:ident, $adapter:ident, $method:ident, $begin:expr) => {{
            sqlx::query(
                "CREATE TABLE gateway_version_test (id INTEGER PRIMARY KEY, title TEXT NOT NULL)",
            )
            .execute(&$pool)
            .await
            .unwrap();
            let adapter = GraphqlPool::$adapter($pool.clone());
            let tables = vec!["gateway_version_test".to_owned()];
            let store = GatewayVersionStore::install(&adapter, "version-test", tables.clone())
                .await
                .unwrap();
            let vector = |v: VersionVector| serde_json::to_value(v).unwrap();
            let empty = vector(store.current(&adapter, &tables).await.unwrap());
            sqlx::query("INSERT INTO gateway_version_test VALUES (1,'first')")
                .execute(&$pool)
                .await
                .unwrap();
            let inserted = vector(store.current(&adapter, &tables).await.unwrap());
            assert_ne!(
                inserted, empty,
                "insert into an empty result invalidates membership"
            );
            let mut failed = $pool.begin().await.unwrap();
            sqlx::query("UPDATE gateway_version_test SET title='rollback'")
                .execute(&mut *failed)
                .await
                .unwrap();
            failed.rollback().await.unwrap();
            assert_eq!(
                vector(store.current(&adapter, &tables).await.unwrap()),
                inserted
            );
            let mut serving = $pool.begin().await.unwrap();
            if let Some(begin) = $begin {
                sqlx::query(sqlx::AssertSqlSafe(begin))
                    .execute(&mut *serving)
                    .await
                    .unwrap();
            }
            let data: String =
                sqlx::query_scalar("SELECT title FROM gateway_version_test WHERE id=1")
                    .fetch_one(&mut *serving)
                    .await
                    .unwrap();
            assert_eq!(data, "first");
            sqlx::query("UPDATE gateway_version_test SET title='newer'")
                .execute(&$pool)
                .await
                .unwrap();
            assert_eq!(
                vector(store.$method(&mut serving, &tables).await.unwrap()),
                inserted,
                "an old result must carry its own old validator, even after concurrent commit"
            );
            serving.commit().await.unwrap();
            let updated = vector(store.current(&adapter, &tables).await.unwrap());
            assert_ne!(updated, inserted);
            sqlx::query("DELETE FROM gateway_version_test WHERE id=1")
                .execute(&$pool)
                .await
                .unwrap();
            assert_ne!(
                vector(store.current(&adapter, &tables).await.unwrap()),
                updated
            );
            assert!(store
                .current(&adapter, &["unknown_dependency".into()])
                .await
                .is_err());
            (adapter, store, tables)
        }};
    }
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn sqlite_transactional_coverage_and_snapshot_race() {
        use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
        let directory =
            std::env::temp_dir().join(format!("gateway-cache-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(directory.clone());
        let pool = SqlitePoolOptions::new()
            .max_connections(3)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(directory.join("cache.sqlite"))
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal),
            )
            .await
            .unwrap();
        let (adapter, store, tables) = exercise!(pool, Sqlite, sqlite, None::<String>);
        let name = store.trigger_name(&tables[0]);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP TRIGGER {}",
            ident(&format!("{name}_UPDATE"))
        )))
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            store.current(&adapter, &tables).await.is_err(),
            "missing hook disables validation"
        );
        sqlx::query("INSERT INTO gateway_version_test VALUES (2,'during missing coverage')")
            .execute(&pool)
            .await
            .unwrap();
        let restored = GatewayVersionStore::install(&adapter, "version-test", tables.clone())
            .await
            .unwrap();
        let before =
            serde_json::to_value(restored.current(&adapter, &tables).await.unwrap()).unwrap();
        restored.rotate_epoch(&adapter).await.unwrap();
        assert_ne!(
            serde_json::to_value(restored.current(&adapter, &tables).await.unwrap()).unwrap(),
            before
        );
    }
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn postgres_transactional_coverage_and_snapshot_race() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(3)
            .connect(
                &std::env::var("GATEWAY_TEST_PRIMARY_URL").expect("run gateway-postgres fixture"),
            )
            .await
            .unwrap();
        let (adapter, store, tables) = exercise!(
            pool,
            Postgres,
            postgres,
            Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY".to_owned())
        );
        let name = store.trigger_name(&tables[0]);
        let before = serde_json::to_value(store.current(&adapter, &tables).await.unwrap()).unwrap();
        sqlx::query("TRUNCATE gateway_version_test")
            .execute(&pool)
            .await
            .unwrap();
        assert_ne!(
            serde_json::to_value(store.current(&adapter, &tables).await.unwrap()).unwrap(),
            before
        );
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE gateway_version_test DISABLE TRIGGER {}",
            ident(&name)
        )))
        .execute(&pool)
        .await
        .unwrap();
        assert!(
            store.current(&adapter, &tables).await.is_err(),
            "disabled hooks cannot certify current data"
        );
        let restored = GatewayVersionStore::install(&adapter, "version-test", tables.clone())
            .await
            .unwrap();
        assert_ne!(
            serde_json::to_value(restored.current(&adapter, &tables).await.unwrap()).unwrap(),
            before
        );
        let before =
            serde_json::to_value(restored.current(&adapter, &tables).await.unwrap()).unwrap();
        restored.rotate_epoch(&adapter).await.unwrap();
        assert_ne!(
            serde_json::to_value(restored.current(&adapter, &tables).await.unwrap()).unwrap(),
            before
        );
    }
}
