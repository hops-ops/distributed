use crate::repository::RepositoryError;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use crate::table::TableStoreError;

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) mod projection_protocol;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) mod read_model;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) mod repo;

#[cfg(feature = "postgres")]
pub(crate) fn repository_i64_from_u64(
    backend: &str,
    value: u64,
    field: &str,
    storage: &str,
) -> Result<i64, RepositoryError> {
    i64::try_from(value).map_err(|_| {
        RepositoryError::Model(format!("{backend} {field} value {value} exceeds {storage}"))
    })
}

pub(crate) fn repository_u64_from_i64(
    backend: &str,
    value: i64,
    field: &str,
) -> Result<u64, RepositoryError> {
    u64::try_from(value)
        .map_err(|_| RepositoryError::Model(format!("{backend} {field} value {value} is negative")))
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn read_model_i64_from_u64(
    backend: &str,
    value: u64,
    field: &str,
    storage: &str,
) -> Result<i64, TableStoreError> {
    i64::try_from(value).map_err(|_| {
        TableStoreError::Storage(format!("{backend} {field} value {value} exceeds {storage}"))
    })
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn read_model_u64_from_i64(
    backend: &str,
    value: i64,
    field: &str,
) -> Result<u64, TableStoreError> {
    u64::try_from(value).map_err(|_| {
        TableStoreError::Storage(format!("{backend} {field} value {value} is negative"))
    })
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn audited_table_schema_sql(statement: String) -> sqlx::AssertSqlSafe<String> {
    // table_schema_statements validates the registry and quotes identifiers before
    // rendering DDL. Schema-authored SQL defaults are the only raw fragments.
    sqlx::AssertSqlSafe(statement)
}

#[cfg(feature = "sqlite")]
pub(crate) fn is_sqlite_unique_constraint(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            let message = db_err.message();
            let code = db_err.code().map(|code| code.into_owned());
            message.contains("UNIQUE constraint failed")
                || message.contains("PRIMARY KEY")
                || matches!(code.as_deref(), Some("1555" | "2067"))
        }
        _ => false,
    }
}

/// Whether a SQLite error is a transient "database is locked"/"busy" condition.
///
/// SQLite serializes writers; without a `busy_timeout` a colliding writer gets
/// `SQLITE_BUSY` (5) / `SQLITE_LOCKED` (6) immediately. For the lease lock that
/// is contention, not failure, so the acquire loop should retry rather than
/// surface it as a `LockError`. Also treats a pool-acquire timeout (e.g. the
/// single-connection `:memory:` pool under contention) as retryable.
#[cfg(feature = "sqlite")]
pub(crate) fn is_sqlite_busy(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            let message = db_err.message().to_ascii_lowercase();
            let code = db_err.code().map(|code| code.into_owned());
            message.contains("database is locked")
                || message.contains("database table is locked")
                || matches!(
                    code.as_deref(),
                    Some("5" | "6" | "261" | "262" | "517" | "518")
                )
        }
        sqlx::Error::PoolTimedOut => true,
        _ => false,
    }
}

#[cfg(feature = "postgres")]
pub(crate) fn is_postgres_unique_violation(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().as_deref() == Some("23505"),
        _ => false,
    }
}

/// Whether a `sqlx::Error` represents a transient condition worth retrying.
///
/// Connection loss, pool exhaustion, and acquire/I/O timeouts are infrastructure
/// hiccups: the same statement may succeed once the backend recovers. SQLite
/// `SQLITE_BUSY`/`SQLITE_LOCKED` contention is likewise transient. Everything
/// else — most notably a `Database` error such as a constraint violation or a
/// malformed-row decode — is deterministic: re-running the identical statement
/// against the same data cannot change the outcome, so it is classified
/// permanent. Treating an unknown failure as permanent is the safe default: a
/// permanent classification hands the message to the failure policy instead of
/// redelivering it forever.
pub(crate) fn is_sqlx_transient(err: &sqlx::Error) -> bool {
    // Connection / pool / timeout failures are transient regardless of backend.
    if matches!(
        err,
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_)
    ) {
        return true;
    }
    // SQLite serializes writers; busy/locked contention is retryable, not failure.
    #[cfg(feature = "sqlite")]
    if is_sqlite_busy(err) {
        return true;
    }
    // Postgres SQLSTATEs that name transient conditions. SQLite never carries
    // these codes (its codes are plain integers), so no feature gate.
    // - 40001 serialization_failure / 40P01 deadlock_detected: the transaction
    //   lost a write race and should be retried, not handed to the failure
    //   policy.
    // - 57P01 admin_shutdown / 57P02 crash_shutdown / 57P03 cannot_connect_now:
    //   the backend was terminated (pg_terminate_backend, failover, restart);
    //   the statement may succeed once the server recovers.
    // - class 08 (connection_exception): the connection died mid-statement.
    if let sqlx::Error::Database(db_err) = err {
        if let Some(code) = db_err.code() {
            if matches!(
                code.as_ref(),
                "40001" | "40P01" | "57P01" | "57P02" | "57P03"
            ) || code.starts_with("08")
            {
                return true;
            }
        }
    }
    false
}

pub(crate) fn repository_storage_error(
    backend: &str,
    operation: &str,
    err: sqlx::Error,
) -> RepositoryError {
    let retryable = is_sqlx_transient(&err);
    RepositoryError::Storage {
        operation: format!("{backend} {operation}"),
        retryable,
        source: Some(Box::new(err)),
    }
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn read_model_storage_error(
    backend: &str,
    operation: &str,
    err: sqlx::Error,
) -> TableStoreError {
    TableStoreError::BackendStorage {
        operation: format!("{backend} {operation}"),
        retryable: is_sqlx_transient(&err),
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::is_sqlx_transient;
    use std::borrow::Cow;
    use std::fmt;

    #[derive(Debug)]
    struct StubDatabaseError(&'static str);

    impl fmt::Display for StubDatabaseError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "stub database error ({})", self.0)
        }
    }

    impl std::error::Error for StubDatabaseError {}

    impl sqlx::error::DatabaseError for StubDatabaseError {
        fn message(&self) -> &str {
            "stub database error"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn database_error(code: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(StubDatabaseError(code)))
    }

    #[test]
    fn write_races_are_transient() {
        assert!(is_sqlx_transient(&database_error("40001")));
        assert!(is_sqlx_transient(&database_error("40P01")));
    }

    #[test]
    fn server_shutdown_and_connection_loss_are_transient() {
        // pg_terminate_backend / failover / restart-in-progress.
        assert!(is_sqlx_transient(&database_error("57P01")));
        assert!(is_sqlx_transient(&database_error("57P02")));
        assert!(is_sqlx_transient(&database_error("57P03")));
        // connection_exception class.
        assert!(is_sqlx_transient(&database_error("08000")));
        assert!(is_sqlx_transient(&database_error("08006")));
    }

    #[test]
    fn deterministic_failures_are_permanent() {
        // unique_violation: re-running the identical statement cannot succeed.
        assert!(!is_sqlx_transient(&database_error("23505")));
        // query_canceled (57014) is a deliberate cancellation, not recovery.
        assert!(!is_sqlx_transient(&database_error("57014")));
        // RowNotFound-style decode errors are permanent.
        assert!(!is_sqlx_transient(&sqlx::Error::RowNotFound));
    }

    #[test]
    fn pool_and_io_failures_are_transient() {
        assert!(is_sqlx_transient(&sqlx::Error::PoolTimedOut));
        assert!(is_sqlx_transient(&sqlx::Error::PoolClosed));
    }
}
