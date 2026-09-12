//! Counter service builder: persistence + lock manager + HTTP helpers.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use distributed::microsvc::{self, Context, HandlerError, HasOutboxStore, Routes, Service};
use distributed::{
    AggregateBuilder, AggregateRepository, GetStream, InMemoryLockManager, InMemoryRepository,
    LockManager, PostgresLockManager, PostgresRepository, Queueable, QueuedRepository,
    SnapshotStore, SqliteLockManager, SqliteRepository, TransactionalCommit,
};
use serde_json::{json, Value};
use tokio::net::TcpListener;

use crate::counter::{counter_state_message, Counter, CreateCounter, IncrementCounter};
use crate::kinds::{compatible_lock, LockKind, RepoKind};

pub const INITIALIZE: &str = "counter.initialize";
pub const INCREMENT: &str = "counter.increment";

type CounterRepo<R, L> = AggregateRepository<QueuedRepository<R, L>, Counter>;

#[derive(Clone, Debug)]
pub struct HostConfig {
    pub repo: RepoKind,
    pub lock: LockKind,
    pub bind: String,
    pub database_url: Option<String>,
    pub sqlite_path: PathBuf,
    /// `None` disables snapshot caching. `Some(n)` calls `with_snapshots(n)`.
    pub snapshot_frequency: Option<u64>,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            repo: RepoKind::Memory,
            lock: LockKind::Memory,
            bind: "127.0.0.1:8790".into(),
            database_url: None,
            sqlite_path: PathBuf::from("target/load.sqlite"),
            snapshot_frequency: None,
        }
    }
}

pub struct BuiltService {
    pub service: Arc<Service>,
    pub sqlite: Option<SqliteRepository>,
    pub postgres: Option<PostgresRepository>,
}

pub struct CounterService {
    pub service: Arc<Service>,
    pub repo: RepoKind,
}

impl CounterService {
    pub async fn start(
        config: &HostConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let built = build_service(config).await?;
        Ok(Self {
            service: built.service,
            repo: config.repo,
        })
    }
}

pub fn build_service_from_sqlite(
    inner: SqliteRepository,
    lock: LockKind,
    snapshot_frequency: Option<u64>,
) -> Result<BuiltService, Box<dyn std::error::Error + Send + Sync>> {
    compatible_lock(RepoKind::Sqlite, lock)?;
    let service = match lock {
        LockKind::Memory => Arc::new(service_for(
            inner.clone(),
            InMemoryLockManager::new(),
            snapshot_frequency,
        )),
        LockKind::Sqlite => Arc::new(service_for(
            inner.clone(),
            SqliteLockManager::new(inner.pool().clone()),
            snapshot_frequency,
        )),
        LockKind::Postgres => {
            return Err("lock=postgres is not valid with repo=sqlite".into());
        }
    };
    Ok(BuiltService {
        service,
        sqlite: Some(inner),
        postgres: None,
    })
}

pub async fn build_service(
    config: &HostConfig,
) -> Result<BuiltService, Box<dyn std::error::Error + Send + Sync>> {
    compatible_lock(config.repo, config.lock)?;
    match config.repo {
        RepoKind::Memory => {
            let inner = InMemoryRepository::new();
            Ok(BuiltService {
                service: Arc::new(service_for(
                    inner,
                    InMemoryLockManager::new(),
                    config.snapshot_frequency,
                )),
                sqlite: None,
                postgres: None,
            })
        }
        RepoKind::Sqlite => {
            let pool_size = if config.lock == LockKind::Sqlite {
                4
            } else {
                1
            };
            let inner = connect_sqlite(&config.sqlite_path, pool_size).await?;
            let service = match config.lock {
                LockKind::Memory => Arc::new(service_for(
                    inner.clone(),
                    InMemoryLockManager::new(),
                    config.snapshot_frequency,
                )),
                LockKind::Sqlite => Arc::new(service_for(
                    inner.clone(),
                    SqliteLockManager::new(inner.pool().clone()),
                    config.snapshot_frequency,
                )),
                LockKind::Postgres => unreachable!("compatible_lock rejects this"),
            };
            Ok(BuiltService {
                service,
                sqlite: Some(inner),
                postgres: None,
            })
        }
        RepoKind::Postgres => {
            let url = postgres_url(config);
            let inner = PostgresRepository::connect_and_migrate(&url).await?;
            let service = match config.lock {
                LockKind::Memory => Arc::new(service_for(
                    inner.clone(),
                    InMemoryLockManager::new(),
                    config.snapshot_frequency,
                )),
                LockKind::Postgres => Arc::new(service_for(
                    inner.clone(),
                    PostgresLockManager::new(inner.pool().clone()),
                    config.snapshot_frequency,
                )),
                LockKind::Sqlite => unreachable!("compatible_lock rejects this"),
            };
            Ok(BuiltService {
                service,
                sqlite: None,
                postgres: Some(inner),
            })
        }
    }
}

pub fn postgres_url(config: &HostConfig) -> String {
    config
        .database_url
        .clone()
        .or_else(|| std::env::var("DATABASE_URL").ok())
        .unwrap_or_else(|| "postgres://sourced:sourced@localhost:5432/distributed".into())
}

pub fn sqlite_pool_size(lock: LockKind, needs_sql_bus: bool) -> u32 {
    if lock == LockKind::Sqlite || needs_sql_bus {
        4
    } else {
        1
    }
}

pub async fn connect_sqlite(
    path: &Path,
    pool_size: u32,
) -> Result<SqliteRepository, Box<dyn std::error::Error + Send + Sync>> {
    use sqlx::sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    };
    use std::str::FromStr;

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(sqlite_sidecar(path, "-wal"));
    let _ = std::fs::remove_file(sqlite_sidecar(path, "-shm"));

    let url = format!("sqlite:{}?mode=rwc", path.display());
    let options = SqliteConnectOptions::from_str(&url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(std::time::Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(pool_size)
        .connect_with(options)
        .await?;
    let repo = SqliteRepository::new(pool);
    repo.migrate().await?;
    Ok(repo)
}

fn sqlite_sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(suffix);
    PathBuf::from(sidecar)
}

#[cfg(test)]
mod path_tests {
    use super::*;

    #[test]
    fn sqlite_sidecars_append_to_the_complete_database_name() {
        let path = Path::new("target/load.custom-db");
        assert_eq!(
            sqlite_sidecar(path, "-wal"),
            PathBuf::from("target/load.custom-db-wal")
        );
        assert_eq!(
            sqlite_sidecar(path, "-shm"),
            PathBuf::from("target/load.custom-db-shm")
        );
    }
}

fn service_for<R, L>(inner: R, locks: L, snapshot_frequency: Option<u64>) -> Service
where
    R: Clone
        + GetStream
        + TransactionalCommit
        + HasOutboxStore
        + SnapshotStore
        + Send
        + Sync
        + 'static,
    L: LockManager + Send + Sync + 'static,
    QueuedRepository<R, L>: Clone
        + GetStream
        + TransactionalCommit
        + HasOutboxStore
        + SnapshotStore
        + Send
        + Sync
        + 'static,
{
    Service::new()
        .named("load-counter")
        .with_http_command_routes()
        .routes(counter_routes(inner, locks, snapshot_frequency))
}

fn counter_repo<R, L>(inner: R, locks: L, snapshot_frequency: Option<u64>) -> CounterRepo<R, L>
where
    R: Clone + GetStream + TransactionalCommit + SnapshotStore + Send + Sync + 'static,
    L: LockManager + Send + Sync + 'static,
    QueuedRepository<R, L>:
        Clone + GetStream + TransactionalCommit + SnapshotStore + Send + Sync + 'static,
{
    let repo = inner.queued_with(locks).aggregate::<Counter>();
    match snapshot_frequency {
        Some(frequency) => repo.with_snapshots(frequency),
        None => repo,
    }
}

fn counter_routes<R, L>(
    inner: R,
    locks: L,
    snapshot_frequency: Option<u64>,
) -> Routes<CounterRepo<R, L>>
where
    R: Clone
        + GetStream
        + TransactionalCommit
        + HasOutboxStore
        + SnapshotStore
        + Send
        + Sync
        + 'static,
    L: LockManager + Send + Sync + 'static,
    QueuedRepository<R, L>: Clone
        + GetStream
        + TransactionalCommit
        + HasOutboxStore
        + SnapshotStore
        + Send
        + Sync
        + 'static,
{
    Routes::new()
        .with_repo(counter_repo(inner, locks, snapshot_frequency))
        .command(INITIALIZE)
        .guarded(guard_create::<R, L>, handle_create::<R, L>)
        .command(INCREMENT)
        .guarded(guard_increment::<R, L>, handle_increment::<R, L>)
}

fn guard_create<R, L>(ctx: &Context<CounterRepo<R, L>>) -> bool {
    ctx.has_fields(&["id"])
}

fn guard_increment<R, L>(ctx: &Context<CounterRepo<R, L>>) -> bool {
    ctx.has_fields(&["id", "amount"])
}

async fn handle_create<R, L>(ctx: &Context<'_, CounterRepo<R, L>>) -> Result<Value, HandlerError>
where
    QueuedRepository<R, L>: GetStream + TransactionalCommit,
{
    let input = ctx.input::<CreateCounter>()?;
    if ctx.repo().get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "counter {} already exists",
            input.id
        )));
    }

    let mut counter = Counter::default();
    counter.create(input.id.clone())?;
    let message = counter_state_message(&counter, "counter.initialized")?;
    ctx.repo().outbox(message).commit(&mut counter).await?;
    Ok(json!({ "id": input.id }))
}

async fn handle_increment<R, L>(ctx: &Context<'_, CounterRepo<R, L>>) -> Result<Value, HandlerError>
where
    QueuedRepository<R, L>: GetStream + TransactionalCommit,
{
    let input = ctx.input::<IncrementCounter>()?;
    let mut counter: Counter = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    counter.increment(input.amount)?;
    let message = counter_state_message(&counter, "counter.incremented")?;
    ctx.repo().outbox(message).commit(&mut counter).await?;
    Ok(json!({ "id": input.id, "value": counter.value }))
}

pub async fn bind_listener(addr: &str) -> Result<TcpListener, std::io::Error> {
    TcpListener::bind(addr).await
}

pub async fn serve_listener(
    service: Arc<Service>,
    listener: TcpListener,
) -> Result<(), std::io::Error> {
    axum::serve(listener, microsvc::router(service)).await
}

pub async fn wait_for_health(base: &str, timeout: std::time::Duration) -> Result<(), String> {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + timeout;
    let url = format!("{base}/health");
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(format!(
                "health check did not succeed at {url} within {timeout:?}"
            ));
        }
        let remaining = deadline - now;
        if let Ok(Ok(resp)) = tokio::time::timeout(remaining, client.get(&url).send()).await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "health check did not succeed at {url} within {timeout:?}"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
