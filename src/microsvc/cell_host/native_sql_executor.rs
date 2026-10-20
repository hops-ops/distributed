//! Native execution of the shared cell SQL repository operations.
//!
//! This is the `cell-sqlite` counterpart of the Durable Object executor in
//! `sql_executor.rs`. Both expose the same `CellSqlConnection::{read,
//! transaction}` surface to `sql_store.rs`, so schema bootstrap, commits,
//! ledger fencing and outbox leases are one shared implementation.
//!
//! # Why a synchronous binding (rusqlite), not SQLx
//!
//! Durable Object `storage.transactionSync` runs the whole command commit in
//! one synchronous callback: nothing can yield between the domain, snapshot,
//! receipt and delivery participants. `rusqlite` gives the same shape natively:
//! `BEGIN IMMEDIATE`, the shared operations completed by `finish_sql` without
//! awaiting, then `COMMIT` (or `ROLLBACK` on any error). An async SQLx pool
//! would need a different, await-based transaction owner and would let the
//! shared store observe a different atomicity contract on each target.
//!
//! One connection per cell file is held behind a mutex; reads and transactions
//! from the same process are serialized just like Durable Object input gates.
//! SQLite calls block the calling thread for their (local, short) duration.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use rusqlite::types::Value;
use rusqlite::Connection;

use crate::repository::sql::{SqlBind, SqlExecutor, SqlPart, SqlRow, Statement};
use crate::repository::{sqlite_codec, RepositoryError};

/// How long a writer waits for another process holding the same cell file.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct CellSqlConnection(Arc<Mutex<Connection>>);

impl CellSqlConnection {
    /// Open (creating when absent) one cell's SQLite file.
    pub fn open(path: &Path) -> Result<Self, RepositoryError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::permanent_storage("create cell directory", error)
            })?;
        }
        let connection = Connection::open(path).map_err(sqlite_error)?;
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(sqlite_error)?;
        // WAL keeps readers off the writer; FULL syncs every commit so an
        // acknowledged command survives power loss, not only process loss.
        connection
            .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get::<_, String>(0))
            .map_err(sqlite_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(sqlite_error)?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // A panic inside a transaction callback left it open. Roll it
                // back so no partial command can be committed by a later one.
                let guard = poisoned.into_inner();
                if !guard.is_autocommit() {
                    let _ = guard.execute_batch("ROLLBACK");
                }
                self.0.clear_poison();
                guard
            }
        }
    }

    /// Run reads outside an explicit transaction, serialized with commits.
    pub fn read<T, E: From<RepositoryError>>(
        &self,
        operation: impl FnOnce(&mut CellSqlExecutor<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut guard = self.lock();
        operation(&mut CellSqlExecutor(&mut guard))
    }

    /// Run `operation` in one `BEGIN IMMEDIATE` transaction. Any error rolls
    /// back every participant; nothing is committed before `COMMIT` returns.
    pub fn transaction<T, E: From<RepositoryError>>(
        &self,
        operation: impl FnOnce(&mut CellSqlExecutor<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut guard = self.lock();
        guard
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(sqlite_error)?;
        match operation(&mut CellSqlExecutor(&mut guard)) {
            Ok(value) => match guard.execute_batch("COMMIT") {
                Ok(()) => Ok(value),
                Err(error) => {
                    let _ = guard.execute_batch("ROLLBACK");
                    Err(sqlite_error(error).into())
                }
            },
            Err(error) => {
                guard.execute_batch("ROLLBACK").map_err(sqlite_error)?;
                Err(error)
            }
        }
    }
}

pub(crate) struct CellSqlExecutor<'a>(&'a mut Connection);
pub(crate) struct CellSqlRow(HashMap<String, Value>);

impl CellSqlRow {
    fn value(&self, column: &str) -> Result<&Value, RepositoryError> {
        self.0
            .get(column)
            .ok_or_else(|| RepositoryError::Model(format!("cell SQL row is missing {column}")))
    }
    fn invalid(column: &str) -> RepositoryError {
        RepositoryError::Model(format!(
            "cell SQL column {column} has the wrong storage type"
        ))
    }
}

impl SqlRow for CellSqlRow {
    fn text(&self, column: &'static str) -> Result<String, RepositoryError> {
        match self.value(column)? {
            Value::Text(value) => Ok(value.clone()),
            _ => Err(Self::invalid(column)),
        }
    }
    fn integer(&self, column: &'static str) -> Result<i64, RepositoryError> {
        match self.value(column)? {
            Value::Integer(value) => Ok(*value),
            _ => Err(Self::invalid(column)),
        }
    }
    fn optional_text(&self, column: &'static str) -> Result<Option<String>, RepositoryError> {
        match self.value(column)? {
            Value::Null => Ok(None),
            _ => self.text(column).map(Some),
        }
    }
    fn optional_timestamp(
        &self,
        column: &'static str,
    ) -> Result<Option<SystemTime>, RepositoryError> {
        match self.value(column)? {
            Value::Null => Ok(None),
            _ => self.timestamp(column).map(Some),
        }
    }
    fn optional_integer(&self, column: &'static str) -> Result<Option<i64>, RepositoryError> {
        match self.value(column)? {
            Value::Null => Ok(None),
            _ => self.integer(column).map(Some),
        }
    }
    fn bytes(&self, column: &'static str) -> Result<Vec<u8>, RepositoryError> {
        match self.value(column)? {
            Value::Blob(value) => Ok(value.clone()),
            _ => Err(Self::invalid(column)),
        }
    }
    fn timestamp(&self, column: &'static str) -> Result<SystemTime, RepositoryError> {
        match self.value(column)? {
            Value::Text(value) => sqlite_codec::decode(value),
            Value::Real(value) => sqlite_codec::decode_epoch(*value),
            Value::Integer(value) => sqlite_codec::decode_epoch(*value as f64),
            _ => Err(Self::invalid(column)),
        }
    }
}

/// Render a structural statement exactly as the Durable Object executor does:
/// same SQLite expressions, `?` placeholders and value storage classes.
fn render(statement: Statement<'_>) -> Result<(String, Vec<Value>), RepositoryError> {
    let mut sql = String::new();
    let mut bindings = Vec::new();
    for part in statement.0 {
        match part {
            SqlPart::TimestampCompare {
                column,
                operator,
                value,
            } => {
                sql.push_str(&format!(
                    "CAST({column} AS REAL) {operator} CAST(? AS REAL)"
                ));
                bindings.push(Value::Text(sqlite_codec::encode(value)?));
            }
            SqlPart::LedgerNow | SqlPart::LedgerNowEpoch => {
                sql.push_str("unixepoch('now','subsec')")
            }
            SqlPart::LedgerDeadline(duration) => {
                sql.push_str("(unixepoch('now','subsec') + ?)");
                bindings.push(Value::Real(duration.as_secs_f64()));
            }
            SqlPart::LedgerDeadlineIsLive(deadline) => {
                sql.push_str("CAST(? AS REAL) > unixepoch('now','subsec')");
                bindings.push(Value::Text(sqlite_codec::encode(deadline)?));
            }
            SqlPart::LedgerJson(json) => {
                sql.push('?');
                bindings.push(Value::Text(json));
            }
            SqlPart::Sql(text) => sql.push_str(&text),
            SqlPart::Bind(value) => {
                sql.push('?');
                bindings.push(match value {
                    SqlBind::Text(value) | SqlBind::Metadata(value) => Value::Text(value),
                    SqlBind::Bytes(value) => Value::Blob(value.into_owned()),
                    SqlBind::Integer(value) => Value::Integer(value),
                    SqlBind::Timestamp(value) => Value::Text(sqlite_codec::encode(value)?),
                });
            }
        }
    }
    Ok((sql, bindings))
}

impl CellSqlExecutor<'_> {
    fn query_rows(&mut self, statement: Statement<'_>) -> Result<Vec<CellSqlRow>, RepositoryError> {
        let (sql, bindings) = render(statement)?;
        let mut prepared = self.0.prepare_cached(&sql).map_err(sqlite_error)?;
        let names = prepared
            .column_names()
            .into_iter()
            .map(String::from)
            .collect::<Vec<_>>();
        let mut rows = prepared
            .query(rusqlite::params_from_iter(bindings))
            .map_err(sqlite_error)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            let mut values = HashMap::with_capacity(names.len());
            for (index, name) in names.iter().enumerate() {
                values.insert(
                    name.clone(),
                    row.get::<_, Value>(index).map_err(sqlite_error)?,
                );
            }
            result.push(CellSqlRow(values));
        }
        Ok(result)
    }

    fn execute_statement(&mut self, statement: Statement<'_>) -> Result<u64, RepositoryError> {
        let (sql, bindings) = render(statement)?;
        if bindings.is_empty() {
            // Validated migrations run as one whole script, as `storage.sql.exec`
            // does; never split SQL text on semicolons.
            self.0.execute_batch(&sql).map_err(sqlite_error)?;
            return Ok(self.0.changes());
        }
        let mut prepared = self.0.prepare_cached(&sql).map_err(sqlite_error)?;
        let written = prepared
            .execute(rusqlite::params_from_iter(bindings))
            .map_err(sqlite_error)?;
        Ok(written as u64)
    }
}

impl SqlExecutor for CellSqlExecutor<'_> {
    type Row = CellSqlRow;
    const EVENT_SELECT: &'static str = sqlite_codec::EVENT_SELECT;
    const SNAPSHOT_SELECT: &'static str = sqlite_codec::SNAPSHOT_SELECT;
    const OUTBOX_SELECT: &'static str = sqlite_codec::OUTBOX_SELECT;
    const NOW: &'static str = "CURRENT_TIMESTAMP";
    const COMMAND_LEDGER_SELECT: &'static str = sqlite_codec::COMMAND_LEDGER_SELECT;
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str = "";
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str = "";

    async fn query(&mut self, statement: Statement<'_>) -> Result<Vec<Self::Row>, RepositoryError> {
        self.query_rows(statement)
    }

    async fn execute(&mut self, statement: Statement<'_>) -> Result<u64, RepositoryError> {
        self.execute_statement(statement)
    }
}

fn sqlite_error(error: rusqlite::Error) -> RepositoryError {
    // Match the Durable Object adapter: storage failures are never allowed to
    // permanently discard a command or pending delivery.
    RepositoryError::retryable_storage("cell SQL", error)
}
