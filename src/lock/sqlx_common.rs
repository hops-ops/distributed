//! Shared, dialect-neutral machinery for the SQLx lease-table locks.
//!
//! The Postgres / SQLite backends differ only in their SQL (placeholder style
//! and the database-clock function). Everything else — the in-process gate,
//! owner-token minting, the acquire poll loop, release, and the manager that
//! hands out cached per-key locks — lives here as [`SqlxLockManager`] /
//! [`SqlxLock`], parameterized by a [`LockDialect`] that supplies the SQL.
//! `PostgresLockManager` / `SqliteLockManager` are type aliases of
//! [`SqlxLockManager`] over their dialect.
//!
//! ## Model
//!
//! Each per-key lock layers an **in-process async gate** ([`InMemoryLock`])
//! over a **durable DB lease** (a row in `aggregate_locks`). The gate serializes
//! same-process tasks with true wakeups (no DB polling between them); only the
//! local gate winner contends on the database, and only against *other
//! processes*. The DB lease carries an `owner_token` (this acquisition's
//! identity) and an `expires_at` derived from the **database clock** (so there is
//! one authoritative clock — no cross-process skew). A lease is stealable once
//! it expires; release is scoped to the owner token so it never stomps a holder
//! that legitimately reclaimed an expired lease.
//!
//! This is a mutual-exclusion *optimization*, not a fencing guarantee: a critical
//! section that outlives `lease_ttl` can be stolen while the original holder
//! still believes it holds the lock. That is safe here only because the event
//! store's `(aggregate_type, aggregate_id, sequence)` primary key is the true
//! concurrency boundary — a stale writer fails its optimistic commit rather than
//! corrupting data. v1 has no lease renewal: set `lease_ttl` above the worst-case
//! critical section.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sqlx::{Database, Encode, FromRow, IntoArguments, Pool, Type};

use super::{InMemoryLock, Lock, LockError, LockManager};

/// Tunables for a lease lock manager. Shared by every per-key lock it hands out.
#[derive(Debug, Clone)]
pub(crate) struct LeaseConfig {
    /// How long an acquired lease stays valid before it becomes stealable.
    pub lease_ttl: Duration,
    /// How long to wait between contended acquire attempts.
    pub retry_interval: Duration,
    /// Optional cap on how long [`lease_lock`] waits before giving up. `None`
    /// waits indefinitely (matching the in-memory lock); the `lease_ttl` still
    /// bounds the wait on a *crashed* holder.
    pub max_wait: Option<Duration>,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_ttl: Duration::from_secs(30),
            retry_interval: Duration::from_millis(50),
            max_wait: None,
        }
    }
}

/// Per-key lock state shared between the acquiring and releasing handles.
///
/// `QueuedRepository` acquires on a locking `get` and releases from a *different*
/// `get_lock(key)` handle on commit/abort; the manager returns the same cached
/// `Arc`, so the gate and the owner token must live here, not in transient
/// handle locals — mirroring `InMemoryLock`'s manager-owned state.
pub(crate) struct LockShared {
    /// In-process gate: same-process tasks serialize here (true wakeups, no DB
    /// polling) so only one local task contends on the DB lease at a time.
    pub gate: InMemoryLock,
    /// The owner token of the lease currently held by this process, if any.
    pub token: Mutex<Option<String>>,
}

impl LockShared {
    pub fn new() -> Self {
        Self {
            gate: InMemoryLock::new(),
            token: Mutex::new(None),
        }
    }
}

/// A dialect-specific lease backend. Implemented by the per-backend locks; the
/// shared [`lease_lock`]/[`lease_try_lock`]/[`lease_unlock`] drive it.
pub(crate) trait LeaseBackend: Send + Sync {
    fn shared(&self) -> &LockShared;
    fn config(&self) -> &LeaseConfig;
    /// Mint a globally-unique token for a new acquisition attempt.
    fn mint_token(&self) -> String;
    /// One atomic conditional-acquire of the lease for the given candidate token.
    /// `Ok(true)` = acquired (or re-acquired our own token); `Ok(false)` =
    /// contended (held by another, not expired) or a transient busy condition.
    fn db_acquire(&self, token: &str) -> impl Future<Output = Result<bool, LockError>> + Send;
    /// Release the lease iff it still carries our token (best-effort).
    fn db_release(&self, token: &str) -> impl Future<Output = Result<(), LockError>> + Send;
}

/// Releases the in-process gate synchronously on drop, unless disarmed.
///
/// Cancellation safety: the lease helpers hold the gate across `.await` points
/// (DB acquire, retry sleep, DB release). If the caller's future is dropped
/// there, only `Drop` runs — so the gate MUST be released from `Drop`, or the
/// key wedges for every later same-process acquire. `Drop` cannot `.await`, so
/// it uses the gate's synchronous `unlock_core`. On the success path the holder
/// keeps the gate, so [`disarm`](Self::disarm) suppresses the release.
struct GateGuard<'a> {
    gate: &'a InMemoryLock,
    armed: bool,
}

impl<'a> GateGuard<'a> {
    fn new(gate: &'a InMemoryLock) -> Self {
        Self { gate, armed: true }
    }

    /// Keep the gate held (the acquisition succeeded); the eventual `unlock`
    /// releases it.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.gate.unlock_core();
        }
    }
}

/// Acquire: hold the in-process gate, then poll the DB lease until won or
/// `max_wait` elapses. The gate is released (via [`GateGuard`]) on every failure
/// path AND on cancellation. `max_wait` is measured from entry, so it also caps
/// the wait for the in-process gate, not just the DB polling.
pub(crate) async fn lease_lock(backend: &impl LeaseBackend) -> Result<(), LockError> {
    let started = Instant::now();
    let max_wait = backend.config().max_wait;

    // Acquire the in-process gate, bounding the wait by `max_wait` if set.
    match max_wait {
        Some(max) => match tokio::time::timeout(max, backend.shared().gate.lock()).await {
            Ok(result) => result?,
            Err(_elapsed) => {
                return Err(LockError::AcquireFailed(format!(
                    "lease acquire timed out after {max:?} waiting for the in-process gate"
                )))
            }
        },
        None => backend.shared().gate.lock().await?,
    }
    let mut guard = GateGuard::new(&backend.shared().gate);

    // One token per acquisition, reused across retries so a lost-response retry
    // can re-acquire our own row (the `OR owner_token = ours` upsert branch).
    let token = backend.mint_token();
    loop {
        match backend.db_acquire(&token).await {
            Ok(true) => {
                store_token(backend.shared(), Some(token));
                guard.disarm(); // success: keep the gate held until `unlock`
                return Ok(());
            }
            Ok(false) => {}
            Err(err) => return Err(err), // guard releases the gate on drop
        }
        if let Some(max) = max_wait {
            if started.elapsed() >= max {
                return Err(LockError::AcquireFailed(format!(
                    "lease acquire timed out after {max:?}"
                )));
            }
        }
        tokio::time::sleep(jittered(backend.config().retry_interval, &token)).await;
    }
}

/// Non-blocking acquire: take the in-process gate if free, then a single DB
/// acquire attempt. The gate is released (via [`GateGuard`]) unless we win.
pub(crate) async fn lease_try_lock(backend: &impl LeaseBackend) -> Result<bool, LockError> {
    if !backend.shared().gate.try_lock().await? {
        return Ok(false);
    }
    let mut guard = GateGuard::new(&backend.shared().gate);
    let token = backend.mint_token();
    match backend.db_acquire(&token).await {
        Ok(true) => {
            store_token(backend.shared(), Some(token));
            guard.disarm();
            Ok(true)
        }
        Ok(false) => Ok(false), // guard releases the gate on drop
        Err(err) => Err(err),   // guard releases the gate on drop
    }
}

/// Release: delete our lease row (best-effort — the lease TTL reclaims it if this
/// fails, and a committed write must never fail on lock cleanup). The in-process
/// gate is released by [`GateGuard`] on drop, so it is freed even if this future
/// is cancelled mid `db_release`. Idempotent if we no longer hold the lease.
pub(crate) async fn lease_unlock(backend: &impl LeaseBackend) -> Result<(), LockError> {
    // Always-armed: this releases the gate on drop whether `db_release` completes,
    // errors, or the future is cancelled while awaiting it.
    let _guard = GateGuard::new(&backend.shared().gate);
    let token = store_token(backend.shared(), None);
    if let Some(token) = token {
        let _ = backend.db_release(&token).await;
    }
    Ok(())
}

/// Swap the stored owner token, returning the previous value. Used to set the
/// token on acquire and take it on release.
///
/// Infallible: the slot holds only an `Option<String>` and is mutated without
/// running user code, so the mutex can never truly be poisoned. We recover the
/// guard defensively rather than return an error.
fn store_token(shared: &LockShared, next: Option<String>) -> Option<String> {
    let mut slot = shared
        .token
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    std::mem::replace(&mut slot, next)
}

/// Per-process owner id: `"{prefix}-{pid}-{nanos}-{mgr_seq}"`. Distinct pids and
/// high-resolution construction time make it unique across processes/restarts;
/// the manager sequence disambiguates managers built in the same process.
pub(crate) fn default_owner_id(prefix: &str) -> String {
    static MANAGER_SEQ: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = MANAGER_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{pid}-{nanos}-{seq}")
}

/// Mint a per-acquisition token unique within this manager: `"{owner_id}:{n}"`.
pub(crate) fn mint_token(owner_id: &str, seq: &AtomicU64) -> String {
    format!("{owner_id}:{}", seq.fetch_add(1, Ordering::Relaxed))
}

/// Spread cross-process waiters by adding up to +25% deterministic jitter
/// derived from the owner token, so distinct holders do not poll in lockstep.
fn jittered(base: Duration, token: &str) -> Duration {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token.hash(&mut hasher);
    let frac = (hasher.finish() % 1000) as f64 / 1000.0;
    base + base.mul_f64(0.25 * frac)
}

/// Map an acquire query error to a `LockError`.
pub(crate) fn lease_acquire_error(err: sqlx::Error) -> LockError {
    LockError::AcquireFailed(format!("sqlx lease acquire failed: {err}"))
}

/// Map a release query error to a `LockError`.
pub(crate) fn lease_release_error(err: sqlx::Error) -> LockError {
    LockError::ReleaseFailed(format!("sqlx lease release failed: {err}"))
}

/// A SQL dialect for the lease lock: the per-backend SQL plus two small hooks.
/// Everything behavioral lives in [`SqlxLockManager`] / [`SqlxLock`]; a dialect
/// is a zero-sized marker that only carries these items.
pub trait LockDialect: Send + Sync + 'static {
    /// The sqlx driver this dialect targets.
    type Db: Database;

    /// Prefix for generated owner ids (`"{prefix}-{pid}-{nanos}-{seq}"`).
    const OWNER_PREFIX: &'static str;
    /// Idempotent `aggregate_locks` DDL, as `;`-separated statements.
    const DDL: &'static str;
    /// One atomic conditional-acquire upsert. Binds `(lock_key, owner_token,
    /// ttl_secs)` and `RETURNING`s the winning `owner_token` — no row when the
    /// lease is held by another, unexpired owner.
    const ACQUIRE_SQL: &'static str;
    /// Owner-scoped lease delete. Binds `(lock_key, owner_token)`.
    const RELEASE_SQL: &'static str;
    /// Expired-lease sweep (no binds).
    const SWEEP_SQL: &'static str;

    /// Whether an acquire error is a transient busy condition to treat as
    /// contention (`Ok(false)`, retried) rather than a failure. SQLite maps
    /// `SQLITE_BUSY` here; Postgres has no equivalent.
    fn busy_is_contention(_err: &sqlx::Error) -> bool {
        false
    }

    /// Rows affected by a statement (sqlx has no dialect-neutral accessor on
    /// [`Database::QueryResult`]).
    fn rows_affected(result: <Self::Db as Database>::QueryResult) -> u64;
}

/// Executes a [`LockDialect`]'s SQL. Blanket-implemented for every dialect
/// whose database supports the lease bind/decode surface (`&str` and `i64`
/// binds, one-`String` rows) — identical across our dialects, so the sqlx
/// bounds live here exactly once and everything else asks for
/// `D: LeaseQueries`.
pub trait LeaseQueries: LockDialect {
    /// One conditional acquire attempt; `Ok(false)` = contended or busy.
    fn acquire(
        pool: &Pool<Self::Db>,
        key: &str,
        token: &str,
        ttl_micros: i64,
    ) -> impl Future<Output = Result<bool, LockError>> + Send;

    /// Release the lease iff it still carries `token` (best-effort).
    fn release(
        pool: &Pool<Self::Db>,
        key: &str,
        token: &str,
    ) -> impl Future<Output = Result<(), LockError>> + Send;

    /// Delete all expired lease rows, returning how many were reclaimed.
    fn sweep(pool: &Pool<Self::Db>) -> impl Future<Output = Result<u64, LockError>> + Send;

    /// Run the idempotent `aggregate_locks` DDL, one statement at a time.
    fn migrate(pool: &Pool<Self::Db>) -> impl Future<Output = Result<(), LockError>> + Send;
}

impl<D> LeaseQueries for D
where
    D: LockDialect,
    for<'c> &'c mut <D::Db as Database>::Connection: sqlx::Executor<'c, Database = D::Db>,
    <D::Db as Database>::Arguments: IntoArguments<D::Db>,
    str: Type<D::Db>,
    for<'q> &'q str: Encode<'q, D::Db>,
    i64: Type<D::Db> + for<'q> Encode<'q, D::Db>,
    for<'r> (String,): FromRow<'r, <D::Db as Database>::Row>,
{
    async fn acquire(
        pool: &Pool<Self::Db>,
        key: &str,
        token: &str,
        ttl_micros: i64,
    ) -> Result<bool, LockError> {
        let outcome = sqlx::query_as::<_, (String,)>(D::ACQUIRE_SQL)
            .bind(key)
            .bind(token)
            .bind(ttl_micros)
            .fetch_optional(pool)
            .await;
        match outcome {
            Ok(row) => Ok(matches!(row, Some((owner,)) if owner == token)),
            Err(err) if D::busy_is_contention(&err) => Ok(false),
            Err(err) => Err(lease_acquire_error(err)),
        }
    }

    async fn release(pool: &Pool<Self::Db>, key: &str, token: &str) -> Result<(), LockError> {
        sqlx::query(D::RELEASE_SQL)
            .bind(key)
            .bind(token)
            .execute(pool)
            .await
            .map_err(lease_release_error)?;
        Ok(())
    }

    async fn sweep(pool: &Pool<Self::Db>) -> Result<u64, LockError> {
        let result = sqlx::query(D::SWEEP_SQL)
            .execute(pool)
            .await
            .map_err(lease_release_error)?;
        Ok(D::rows_affected(result))
    }

    async fn migrate(pool: &Pool<Self::Db>) -> Result<(), LockError> {
        for statement in D::DDL.split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            sqlx::query(statement).execute(pool).await.map_err(|err| {
                LockError::Other(format!("migrate aggregate_locks failed: {err}"))
            })?;
        }
        Ok(())
    }
}

/// Durable SQLx [`LockManager`]: hands out one cached [`SqlxLock`] per key
/// (like `InMemoryLockManager`), each persisting a per-stream lease in the
/// `aggregate_locks` table. The dialect `D` supplies the SQL.
///
/// Apply the `with_*` tunables **before the first [`get_lock`](LockManager::get_lock)**:
/// each per-key lock captures the configuration at creation time, so reconfiguring
/// after locks have been handed out would not affect the already-cached ones.
pub struct SqlxLockManager<D: LockDialect> {
    pool: Pool<D::Db>,
    owner_id: String,
    config: LeaseConfig,
    token_seq: Arc<AtomicU64>,
    locks: Arc<Mutex<HashMap<String, Arc<SqlxLock<D>>>>>,
}

// Manual impl: `D` is a marker type that never needs to be `Clone` itself.
impl<D: LockDialect> Clone for SqlxLockManager<D> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            owner_id: self.owner_id.clone(),
            config: self.config.clone(),
            token_seq: Arc::clone(&self.token_seq),
            locks: Arc::clone(&self.locks),
        }
    }
}

impl<D: LockDialect> SqlxLockManager<D> {
    /// Create a manager over an existing (migrated) pool, with default tunables
    /// (30s lease TTL, 50ms retry interval, wait indefinitely).
    pub fn new(pool: Pool<D::Db>) -> Self {
        Self {
            pool,
            owner_id: default_owner_id(D::OWNER_PREFIX),
            config: LeaseConfig::default(),
            token_seq: Arc::new(AtomicU64::new(0)),
            locks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set how long an acquired lease stays valid before it becomes stealable.
    /// Must exceed the worst-case critical section (v1 has no renewal).
    pub fn with_lease_ttl(mut self, ttl: Duration) -> Self {
        self.config.lease_ttl = ttl;
        self
    }

    /// Set the wait between contended acquire attempts.
    pub fn with_retry_interval(mut self, interval: Duration) -> Self {
        self.config.retry_interval = interval;
        self
    }

    /// Cap how long `lock` waits before failing with `AcquireFailed`. `None`
    /// (the default) waits indefinitely.
    pub fn with_max_wait(mut self, max_wait: Option<Duration>) -> Self {
        self.config.max_wait = max_wait;
        self
    }

    /// Override the owner id (otherwise a process-unique id is generated).
    /// Useful for observability; must stay unique per process.
    pub fn with_owner_id(mut self, owner_id: impl Into<String>) -> Self {
        self.owner_id = owner_id.into();
        self
    }
}

impl<D: LeaseQueries> SqlxLockManager<D> {
    /// Create the `aggregate_locks` table for standalone use (no repository).
    pub async fn migrate(pool: &Pool<D::Db>) -> Result<(), LockError> {
        D::migrate(pool).await
    }

    /// Delete all expired lease rows; returns how many were reclaimed. Optional
    /// GC for keys that were locked once and never again (acquire already reuses
    /// the row for live keys).
    pub async fn sweep_expired(&self) -> Result<u64, LockError> {
        D::sweep(&self.pool).await
    }
}

impl<D: LeaseQueries> LockManager for SqlxLockManager<D> {
    type Lock = SqlxLock<D>;

    fn get_lock(&self, id: &str) -> Result<Arc<SqlxLock<D>>, LockError> {
        let mut locks = self
            .locks
            .lock()
            .map_err(|_| LockError::Poisoned("sqlx lock manager map poisoned".into()))?;
        Ok(locks
            .entry(id.to_string())
            .or_insert_with(|| {
                Arc::new(SqlxLock {
                    pool: self.pool.clone(),
                    owner_id: self.owner_id.clone(),
                    config: self.config.clone(),
                    token_seq: Arc::clone(&self.token_seq),
                    key: id.to_string(),
                    shared: LockShared::new(),
                    dialect: PhantomData,
                })
            })
            .clone())
    }
}

/// A single durable lease lock for one stream key (see the module docs for the
/// lease model). Handed out — cached — by [`SqlxLockManager::get_lock`].
pub struct SqlxLock<D: LockDialect> {
    pool: Pool<D::Db>,
    owner_id: String,
    config: LeaseConfig,
    token_seq: Arc<AtomicU64>,
    key: String,
    shared: LockShared,
    dialect: PhantomData<D>,
}

impl<D: LeaseQueries> LeaseBackend for SqlxLock<D> {
    fn shared(&self) -> &LockShared {
        &self.shared
    }

    fn config(&self) -> &LeaseConfig {
        &self.config
    }

    fn mint_token(&self) -> String {
        mint_token(&self.owner_id, &self.token_seq)
    }

    async fn db_acquire(&self, token: &str) -> Result<bool, LockError> {
        D::acquire(
            &self.pool,
            &self.key,
            token,
            // A TTL past any representable time never expires; clamping
            // keeps `now + ttl` inside the integer range.
            crate::repository::sql::duration_micros(self.config.lease_ttl).unwrap_or(i64::MAX / 2),
        )
        .await
    }

    async fn db_release(&self, token: &str) -> Result<(), LockError> {
        D::release(&self.pool, &self.key, token).await
    }
}

impl<D: LeaseQueries> Lock for SqlxLock<D> {
    fn lock(&self) -> impl Future<Output = Result<(), LockError>> + Send + '_ {
        lease_lock(self)
    }

    fn try_lock(&self) -> impl Future<Output = Result<bool, LockError>> + Send + '_ {
        lease_try_lock(self)
    }

    fn unlock(&self) -> impl Future<Output = Result<(), LockError>> + Send + '_ {
        lease_unlock(self)
    }

    fn release_detached(self: Arc<Self>) {
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    let _ = lease_unlock(&*self).await;
                });
            }
            // No runtime to delete the lease row on: free this process's
            // gate now and let the row expire by its TTL.
            Err(_) => {
                store_token(self.shared(), None);
                let _ = self.shared().gate.unlock_core();
            }
        }
    }
}
