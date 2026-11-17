use super::*;

/// SQL-backed async repository generic over the SQLx backend.
///
/// Use through the public aliases: [`PostgresRepository`](crate::PostgresRepository)
/// and [`SqliteRepository`](crate::SqliteRepository).
pub struct SqlxRepository<DB: sqlx::Database> {
    pub(super) pool: Pool<DB>,
    pub(super) read_model_schemas: Arc<RwLock<TableSchemaRegistry>>,
    pub(super) read_model_change_tx: tokio::sync::broadcast::Sender<crate::ReadModelChange>,
    /// When false, skips Postgres `pg_notify` (local broadcast still fires).
    /// Opt-out via [`SqlxRepository::without_read_model_change_notify`]. Writers
    /// that opt out silently break cross-process GraphQL subscriptions.
    pub(super) notify_enabled: bool,
    pub(super) projection_change_retention: ProjectionChangeRetention,
    pub(super) causal_storage_identity: CausalStorageIdentity,
    /// A [`ScheduleWorker`](crate::schedule::worker::ScheduleWorker) was
    /// attached (`with_schedule_worker`): routes may require
    /// [`Capability::Scheduling`](crate::schedule::Capability::Scheduling).
    pub(super) schedule_dispatch: bool,
}

impl<DB: sqlx::Database> Clone for SqlxRepository<DB> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            read_model_schemas: Arc::clone(&self.read_model_schemas),
            read_model_change_tx: self.read_model_change_tx.clone(),
            notify_enabled: self.notify_enabled,
            projection_change_retention: self.projection_change_retention,
            causal_storage_identity: self.causal_storage_identity,
            schedule_dispatch: self.schedule_dispatch,
        }
    }
}

/// SQL-backed outbox table store generic over the SQLx backend.
///
/// Use through the public aliases: [`PostgresOutboxStore`](crate::PostgresOutboxStore)
/// and [`SqliteOutboxStore`](crate::SqliteOutboxStore).
pub struct SqlxOutboxStore<DB: sqlx::Database> {
    pub(super) pool: Pool<DB>,
}

impl<DB: sqlx::Database> Clone for SqlxOutboxStore<DB> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

impl<DB> SqlxRepository<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
{
    /// Create a repository from an existing migrated pool.
    pub fn new(pool: Pool<DB>) -> Self {
        let (read_model_change_tx, _) = tokio::sync::broadcast::channel(256);
        Self {
            pool,
            read_model_schemas: Arc::new(RwLock::new(TableSchemaRegistry::new())),
            read_model_change_tx,
            notify_enabled: true,
            projection_change_retention: ProjectionChangeRetention::default(),
            causal_storage_identity: CausalStorageIdentity::new(),
            schedule_dispatch: false,
        }
    }

    #[cfg_attr(not(feature = "graphql"), allow(dead_code))]
    pub(crate) fn causal_storage_identity(&self) -> CausalStorageIdentity {
        self.causal_storage_identity
    }

    /// Subscribe to read-model table changes (fires after successful write-plan commits).
    ///
    /// Lagging receivers observe [`tokio::sync::broadcast::error::RecvError::Lagged`]
    /// and should treat that as all-dirty for subscription invalidation.
    pub fn read_model_changes(&self) -> tokio::sync::broadcast::Receiver<crate::ReadModelChange> {
        self.read_model_change_tx.subscribe()
    }

    /// Disable Postgres `pg_notify` emission on read-model commits (local broadcast
    /// remains active). Default is ON.
    ///
    /// **Failure mode:** writer processes that opt out silently break
    /// cross-process GraphQL subscriptions that rely on LISTEN/NOTIFY.
    pub fn without_read_model_change_notify(mut self) -> Self {
        self.notify_enabled = false;
        self
    }

    /// Configure the maximum newest projection changes retained per partition.
    ///
    /// Lengthening this value never restores a prefix already represented by
    /// the durable compacted-through watermark.
    pub fn with_projection_change_retention(
        mut self,
        retention: ProjectionChangeRetention,
    ) -> Self {
        self.projection_change_retention = retention;
        self
    }

    pub fn publish_read_model_change(&self, change: crate::ReadModelChange) {
        if change.is_empty() {
            return;
        }
        // Zero receivers is a no-op (broadcast::send returns Err).
        let _ = self.read_model_change_tx.send(change);
    }

    pub(crate) fn projection_notify_enabled(&self) -> bool {
        self.notify_enabled
    }

    pub(crate) fn projection_change_retention(&self) -> ProjectionChangeRetention {
        self.projection_change_retention
    }

    /// Open a pool without applying migrations.
    pub async fn connect(database_url: &str) -> Result<Self, RepositoryError> {
        let pool = PoolOptions::<DB>::new()
            .max_connections(DB::default_pool_size(database_url))
            .connect(database_url)
            .await
            .map_err(|err| repository_storage_error::<DB>("connect", err))?;
        Ok(Self::new(pool))
    }

    /// Open a pool and apply this backend's explicit migrations.
    pub async fn connect_and_migrate(database_url: &str) -> Result<Self, RepositoryError>
    where
        DB::Connection: Migrate,
    {
        let repo = Self::connect(database_url).await?;
        repo.migrate().await?;
        Ok(repo)
    }

    /// Apply this backend's migrations to the repository's pool.
    pub async fn migrate(&self) -> Result<(), RepositoryError>
    where
        DB::Connection: Migrate,
    {
        Self::migrate_pool(&self.pool).await
    }

    /// Apply this backend's migrations to an existing pool. Applied versions
    /// are recorded in the `_sqlx_migrations` ledger, so re-running is a no-op
    /// and an edited already-applied migration fails its checksum comparison
    /// instead of silently diverging.
    pub async fn migrate_pool(pool: &Pool<DB>) -> Result<(), RepositoryError>
    where
        DB::Connection: Migrate,
    {
        DB::migrator()
            .run(pool)
            .await
            .map_err(|err| RepositoryError::Storage {
                operation: format!("{} migrate", DB::BACKEND),
                retryable: false,
                source: Some(Box::new(err)),
            })
    }

    /// Access the underlying SQLx pool for application-specific setup or tests.
    pub fn pool(&self) -> &Pool<DB> {
        &self.pool
    }

    /// SQL artifact adapter for registered table/read-model schemas.
    pub fn table_schema_adapter(&self) -> TableSqlSchemaAdapter {
        table_schema_adapter::<DB>()
    }

    /// Generate SQL statements for registered table/read-model schemas.
    pub fn generate_table_migration_artifacts(
        &self,
        registry: &TableSchemaRegistry,
    ) -> Result<Vec<TableMigrationArtifact>, TableStoreError> {
        generate_table_migration_artifacts(registry, DB::TABLE_DIALECT)
    }

    /// Explicit dev/test bootstrap for registered table/read-model schemas.
    pub async fn bootstrap_table_schema_for_dev(
        &self,
        registry: &TableSchemaRegistry,
    ) -> Result<TableSchemaBootstrap, TableStoreError> {
        bootstrap_table_schema(&self.pool, registry).await?;
        remember_read_model_schemas(&self.read_model_schemas, registry)?;
        Ok(table_schema_bootstrap_result(registry))
    }

    /// Access an outbox-store handle backed by this repository's pool.
    pub fn outbox_store(&self) -> SqlxOutboxStore<DB> {
        SqlxOutboxStore {
            pool: self.pool.clone(),
        }
    }
}

impl<DB> SqlxOutboxStore<DB>
where
    DB: SqlxRepoBackend,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
{
    pub fn new(pool: Pool<DB>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Pool<DB> {
        &self.pool
    }

    /// SQL artifact adapter for registered table/read-model schemas.
    pub fn table_schema_adapter(&self) -> TableSqlSchemaAdapter {
        table_schema_adapter::<DB>()
    }

    /// Generate SQL statements for registered table/read-model schemas.
    pub fn generate_table_migration_artifacts(
        &self,
        registry: &TableSchemaRegistry,
    ) -> Result<Vec<TableMigrationArtifact>, TableStoreError> {
        generate_table_migration_artifacts(registry, DB::TABLE_DIALECT)
    }

    /// Explicit dev/test bootstrap for registered table/read-model schemas.
    pub async fn bootstrap_table_schema_for_dev(
        &self,
        registry: &TableSchemaRegistry,
    ) -> Result<TableSchemaBootstrap, TableStoreError> {
        bootstrap_table_schema(&self.pool, registry).await?;
        Ok(table_schema_bootstrap_result(registry))
    }
}

fn table_schema_adapter<DB: SqlxRepoBackend>() -> TableSqlSchemaAdapter {
    match DB::TABLE_DIALECT {
        TableSqlDialect::Postgres => TableSqlSchemaAdapter::postgres(),
        TableSqlDialect::Sqlite => TableSqlSchemaAdapter::sqlite(),
    }
}

async fn bootstrap_table_schema<DB>(
    pool: &Pool<DB>,
    registry: &TableSchemaRegistry,
) -> Result<(), TableStoreError>
where
    DB: SqlxRepoBackend,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    DB::Arguments: IntoArguments<DB>,
{
    for statement in table_schema_statements(registry, DB::TABLE_DIALECT)? {
        sqlx::query(audited_table_schema_sql(statement))
            .execute(pool)
            .await
            .map_err(|err| {
                TableStoreError::Storage(format!(
                    "{} bootstrap table schema failed: {err}",
                    DB::BACKEND
                ))
            })?;
    }
    Ok(())
}
