//! Durable SQLx lease stores (`coordination_leases` table).
//!
//! Mirrors the [`crate::lock`] SQLx layout: dialect-neutral machinery in
//! [`SqlxLeaseStore`], per-backend SQL in a zero-sized [`LeaseDialect`]
//! marker. All expiry decisions use the **database clock**
//! (`unixepoch('now','subsec')` / `extract(epoch from now())`), so processes
//! with skewed clocks still agree on who holds a lease.
//!
//! Each statement is a single atomic conditional write:
//!
//! - acquire = upsert that inserts token 1, or takes over an expired/released
//!   row with `fencing_token + 1`, and is a no-op on a live row;
//! - renew = update guarded by `(holder, fencing_token, unexpired)`;
//! - release = the same guard, setting `expires_at = 0` and **keeping** the
//!   row so the token never resets.
#![expect(
    clippy::manual_async_fn,
    reason = "trait impls return impl Future + Send to preserve public Send bounds"
)]

use std::future::Future;
use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sqlx::{Database, Encode, FromRow, IntoArguments, Pool, Type};

use super::{validate_request, FencingToken, Lease, LeaseError, LeaseStore};

/// Per-backend SQL for [`SqlxLeaseStore`].
pub trait LeaseDialect: Send + Sync + 'static {
    /// The sqlx driver this dialect targets.
    type Db: Database;

    /// Idempotent `coordination_leases` DDL, `;`-separated. The repository
    /// migration `0010_coordination_leases` creates the same table.
    const DDL: &'static str;
    /// Binds `(key, holder, ttl_secs)`; `RETURNING holder, fencing_token,
    /// expires_at` only when this call acquired.
    const ACQUIRE_SQL: &'static str;
    /// Binds `(key, holder, token, ttl_secs)`; `RETURNING holder,
    /// fencing_token, expires_at` only when the lease was live and ours.
    const RENEW_SQL: &'static str;
    /// Binds `(key, holder, token)`; `RETURNING holder, fencing_token,
    /// expires_at` only when the lease was live and ours.
    const RELEASE_SQL: &'static str;
    /// Binds `(key)`; returns `holder, fencing_token, expires_at, db_now`.
    const CURRENT_SQL: &'static str;

    /// Whether an error is a transient busy condition (SQLite `SQLITE_BUSY`).
    fn is_busy(_err: &sqlx::Error) -> bool {
        false
    }
}

type LeaseRow = (String, i64, f64);
type CurrentRow = (String, i64, f64, f64);

/// Durable [`LeaseStore`] over a SQLx pool. Cheap to clone.
pub struct SqlxLeaseStore<D: LeaseDialect> {
    pool: Pool<D::Db>,
    dialect: PhantomData<D>,
}

impl<D: LeaseDialect> Clone for SqlxLeaseStore<D> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            dialect: PhantomData,
        }
    }
}

impl<D: LeaseDialect> SqlxLeaseStore<D> {
    /// A store over an existing pool. The pool's database must already have
    /// the `coordination_leases` table (repository migrations or
    /// [`migrate`](Self::migrate)).
    pub fn new(pool: Pool<D::Db>) -> Self {
        Self {
            pool,
            dialect: PhantomData,
        }
    }

    /// The underlying pool.
    pub fn pool(&self) -> &Pool<D::Db> {
        &self.pool
    }
}

fn backend_error<D: LeaseDialect>(op: &str, err: sqlx::Error) -> LeaseError {
    if D::is_busy(&err) {
        LeaseError::Backend(format!("lease {op} busy: {err}"))
    } else {
        LeaseError::Backend(format!("lease {op} failed: {err}"))
    }
}

fn epoch_to_time(secs: f64) -> SystemTime {
    if secs.is_finite() && secs > 0.0 {
        UNIX_EPOCH + Duration::from_secs_f64(secs)
    } else {
        UNIX_EPOCH
    }
}

fn lease_from_row(key: &str, (holder, token, expires_at): LeaseRow) -> Result<Lease, LeaseError> {
    let token = u64::try_from(token)
        .map_err(|_| LeaseError::Backend(format!("invalid stored fencing token {token}")))?;
    Ok(Lease {
        key: key.to_string(),
        holder,
        token: FencingToken::new(token),
        expires_at: epoch_to_time(expires_at),
    })
}

fn token_bind(lease: &Lease) -> Result<i64, LeaseError> {
    i64::try_from(lease.token.get())
        .map_err(|_| LeaseError::Invalid("fencing token exceeds storage range".into()))
}

impl<D> SqlxLeaseStore<D>
where
    D: LeaseDialect,
    for<'c> &'c Pool<D::Db>: sqlx::Executor<'c, Database = D::Db>,
    <D::Db as Database>::Arguments: IntoArguments<D::Db>,
    for<'q> &'q str: Encode<'q, D::Db> + Type<D::Db>,
    for<'q> f64: Encode<'q, D::Db> + Type<D::Db>,
    for<'q> i64: Encode<'q, D::Db> + Type<D::Db>,
    for<'r> LeaseRow: FromRow<'r, <D::Db as Database>::Row>,
    for<'r> CurrentRow: FromRow<'r, <D::Db as Database>::Row>,
{
    /// Create the `coordination_leases` table for standalone use.
    pub async fn migrate(pool: &Pool<D::Db>) -> Result<(), LeaseError> {
        for statement in D::DDL.split(';') {
            let statement = statement.trim();
            if statement.is_empty() {
                continue;
            }
            sqlx::query(statement)
                .execute(pool)
                .await
                .map_err(|err| backend_error::<D>("migrate", err))?;
        }
        Ok(())
    }

    async fn current_row(&self, key: &str) -> Result<Option<(Lease, SystemTime)>, LeaseError> {
        let row = sqlx::query_as::<_, CurrentRow>(D::CURRENT_SQL)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| backend_error::<D>("read", err))?;
        row.map(|(holder, token, expires_at, now)| {
            Ok((
                lease_from_row(key, (holder, token, expires_at))?,
                epoch_to_time(now),
            ))
        })
        .transpose()
    }
}

impl<D> LeaseStore for SqlxLeaseStore<D>
where
    D: LeaseDialect,
    for<'c> &'c Pool<D::Db>: sqlx::Executor<'c, Database = D::Db>,
    <D::Db as Database>::Arguments: IntoArguments<D::Db>,
    for<'q> &'q str: Encode<'q, D::Db> + Type<D::Db>,
    for<'q> f64: Encode<'q, D::Db> + Type<D::Db>,
    for<'q> i64: Encode<'q, D::Db> + Type<D::Db>,
    for<'r> LeaseRow: FromRow<'r, <D::Db as Database>::Row>,
    for<'r> CurrentRow: FromRow<'r, <D::Db as Database>::Row>,
{
    fn acquire<'a>(
        &'a self,
        key: &'a str,
        holder: &'a str,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, LeaseError>> + Send + 'a {
        async move {
            validate_request(key, holder, ttl)?;
            let acquired = sqlx::query_as::<_, LeaseRow>(D::ACQUIRE_SQL)
                .bind(key)
                .bind(holder)
                .bind(ttl.as_secs_f64())
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| backend_error::<D>("acquire", err))?;
            if let Some(row) = acquired {
                return lease_from_row(key, row);
            }
            // Live row: ours (idempotent retry) or someone else's.
            match self.current_row(key).await? {
                Some((lease, now)) if lease.expires_at > now => {
                    if lease.holder == holder {
                        Ok(lease)
                    } else {
                        Err(LeaseError::Held {
                            key: key.to_string(),
                            holder: lease.holder,
                            expires_at: lease.expires_at,
                        })
                    }
                }
                // Expired between the two statements: report contention and
                // let the caller retry rather than loop here.
                _ => Err(LeaseError::Backend(format!(
                    "lease `{key}` changed during acquire; retry"
                ))),
            }
        }
    }

    fn renew<'a>(
        &'a self,
        lease: &'a Lease,
        ttl: Duration,
    ) -> impl Future<Output = Result<Lease, LeaseError>> + Send + 'a {
        async move {
            validate_request(&lease.key, &lease.holder, ttl)?;
            let row = sqlx::query_as::<_, LeaseRow>(D::RENEW_SQL)
                .bind(lease.key.as_str())
                .bind(lease.holder.as_str())
                .bind(token_bind(lease)?)
                .bind(ttl.as_secs_f64())
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| backend_error::<D>("renew", err))?;
            match row {
                Some(row) => lease_from_row(&lease.key, row),
                None => Err(LeaseError::Lost {
                    key: lease.key.clone(),
                }),
            }
        }
    }

    fn release<'a>(
        &'a self,
        lease: &'a Lease,
    ) -> impl Future<Output = Result<bool, LeaseError>> + Send + 'a {
        async move {
            let row = sqlx::query_as::<_, LeaseRow>(D::RELEASE_SQL)
                .bind(lease.key.as_str())
                .bind(lease.holder.as_str())
                .bind(token_bind(lease)?)
                .fetch_optional(&self.pool)
                .await
                .map_err(|err| backend_error::<D>("release", err))?;
            Ok(row.is_some())
        }
    }

    fn current<'a>(
        &'a self,
        key: &'a str,
    ) -> impl Future<Output = Result<Option<Lease>, LeaseError>> + Send + 'a {
        async move {
            Ok(self
                .current_row(key)
                .await?
                .filter(|(lease, now)| lease.expires_at > *now)
                .map(|(lease, _)| lease))
        }
    }
}

// ---------------------------------------------------------------------------
// SQLite
// ---------------------------------------------------------------------------

/// SQLite [`LeaseDialect`]: `?n` placeholders, `unixepoch('now','subsec')`.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, Copy)]
pub struct SqliteLeaseDialect;

#[cfg(feature = "sqlite")]
impl LeaseDialect for SqliteLeaseDialect {
    type Db = sqlx::Sqlite;

    const DDL: &'static str = "\
CREATE TABLE IF NOT EXISTS coordination_leases (\
  lease_key TEXT NOT NULL PRIMARY KEY,\
  holder TEXT NOT NULL,\
  fencing_token INTEGER NOT NULL,\
  acquired_at REAL NOT NULL,\
  expires_at REAL NOT NULL,\
  CHECK (lease_key <> ''),\
  CHECK (holder <> ''),\
  CHECK (fencing_token > 0)\
)";

    const ACQUIRE_SQL: &'static str = r#"
        INSERT INTO coordination_leases (lease_key, holder, fencing_token, acquired_at, expires_at)
        VALUES (?1, ?2, 1, unixepoch('now','subsec'), unixepoch('now','subsec') + ?3)
        ON CONFLICT (lease_key) DO UPDATE
          SET holder = excluded.holder,
              fencing_token = coordination_leases.fencing_token + 1,
              acquired_at = excluded.acquired_at,
              expires_at = excluded.expires_at
          WHERE coordination_leases.expires_at <= unixepoch('now','subsec')
        RETURNING holder, fencing_token, expires_at
        "#;

    const RENEW_SQL: &'static str = r#"
        UPDATE coordination_leases
           SET expires_at = unixepoch('now','subsec') + ?4
         WHERE lease_key = ?1 AND holder = ?2 AND fencing_token = ?3
           AND expires_at > unixepoch('now','subsec')
        RETURNING holder, fencing_token, expires_at
        "#;

    const RELEASE_SQL: &'static str = r#"
        UPDATE coordination_leases
           SET expires_at = 0
         WHERE lease_key = ?1 AND holder = ?2 AND fencing_token = ?3
           AND expires_at > unixepoch('now','subsec')
        RETURNING holder, fencing_token, expires_at
        "#;

    const CURRENT_SQL: &'static str = r#"
        SELECT holder, fencing_token, expires_at, unixepoch('now','subsec') AS db_now
          FROM coordination_leases
         WHERE lease_key = ?1
        "#;

    fn is_busy(err: &sqlx::Error) -> bool {
        crate::sqlx_repo::is_sqlite_busy(err)
    }
}

/// SQLite-backed [`LeaseStore`]. Cross-process use needs a shared file DB.
#[cfg(feature = "sqlite")]
pub type SqliteLeaseStore = SqlxLeaseStore<SqliteLeaseDialect>;

// ---------------------------------------------------------------------------
// Postgres
// ---------------------------------------------------------------------------

/// Postgres [`LeaseDialect`]: `$n` placeholders, `extract(epoch from now())`.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone, Copy)]
pub struct PostgresLeaseDialect;

#[cfg(feature = "postgres")]
impl LeaseDialect for PostgresLeaseDialect {
    type Db = sqlx::Postgres;

    const DDL: &'static str = "\
CREATE TABLE IF NOT EXISTS coordination_leases (\
  lease_key text NOT NULL PRIMARY KEY,\
  holder text NOT NULL,\
  fencing_token bigint NOT NULL,\
  acquired_at double precision NOT NULL,\
  expires_at double precision NOT NULL,\
  CHECK (lease_key <> ''),\
  CHECK (holder <> ''),\
  CHECK (fencing_token > 0)\
)";

    const ACQUIRE_SQL: &'static str = r#"
        INSERT INTO coordination_leases (lease_key, holder, fencing_token, acquired_at, expires_at)
        VALUES ($1, $2, 1,
                extract(epoch from clock_timestamp())::double precision,
                extract(epoch from clock_timestamp())::double precision + $3)
        ON CONFLICT (lease_key) DO UPDATE
          SET holder = EXCLUDED.holder,
              fencing_token = coordination_leases.fencing_token + 1,
              acquired_at = EXCLUDED.acquired_at,
              expires_at = EXCLUDED.expires_at
          WHERE coordination_leases.expires_at <= extract(epoch from clock_timestamp())::double precision
        RETURNING holder, fencing_token, expires_at
        "#;

    const RENEW_SQL: &'static str = r#"
        UPDATE coordination_leases
           SET expires_at = extract(epoch from clock_timestamp())::double precision + $4
         WHERE lease_key = $1 AND holder = $2 AND fencing_token = $3
           AND expires_at > extract(epoch from clock_timestamp())::double precision
        RETURNING holder, fencing_token, expires_at
        "#;

    const RELEASE_SQL: &'static str = r#"
        UPDATE coordination_leases
           SET expires_at = 0
         WHERE lease_key = $1 AND holder = $2 AND fencing_token = $3
           AND expires_at > extract(epoch from clock_timestamp())::double precision
        RETURNING holder, fencing_token, expires_at
        "#;

    const CURRENT_SQL: &'static str = r#"
        SELECT holder, fencing_token, expires_at,
               extract(epoch from clock_timestamp())::double precision AS db_now
          FROM coordination_leases
         WHERE lease_key = $1
        "#;
}

/// Postgres-backed [`LeaseStore`].
#[cfg(feature = "postgres")]
pub type PostgresLeaseStore = SqlxLeaseStore<PostgresLeaseDialect>;
