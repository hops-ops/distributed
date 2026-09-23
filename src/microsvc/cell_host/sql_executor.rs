//! Durable Object execution of the shared SQL repository operations.
//!
//! No event serialization, replay, or snapshot policy lives in this adapter.
//! The callback is synchronous and bounded by storage.transactionSync, so a
//! commit cannot yield between its domain and delivery/receipt participants.

use std::collections::HashMap;
use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::SystemTime;

use worker::js_sys::{Function, Reflect};
use worker::send::SendWrapper;
use worker::wasm_bindgen::{closure::ScopedClosure, JsCast, JsValue};
use worker::{SqlStorage, SqlStorageValue, State};

use crate::repository::sql::{SqlBind, SqlExecutor, SqlPart, SqlRow, Statement};
use crate::repository::{sqlite_codec, RepositoryError};

#[derive(Clone)]
pub(crate) struct CellSqlConnection {
    sql: SqlStorage,
    storage: SendWrapper<JsValue>,
    transaction_sync: SendWrapper<Function>,
}

impl CellSqlConnection {
    /// Takes the runtime-owned state, not a caller-supplied database URL.
    pub fn from_state(state: State) -> Result<Self, RepositoryError> {
        let sql = state.storage().sql();
        let storage: JsValue = state._inner().storage().map_err(js_error)?.into();
        let transaction_sync = Reflect::get(&storage, &JsValue::from_str("transactionSync"))
            .map_err(js_error)?
            .dyn_into::<Function>()
            .map_err(|_| {
                RepositoryError::Model("cell storage.transactionSync is required".into())
            })?;
        Ok(Self {
            sql,
            storage: SendWrapper::new(storage),
            transaction_sync: SendWrapper::new(transaction_sync),
        })
    }

    pub fn executor(&self) -> CellSqlExecutor {
        CellSqlExecutor(self.sql.clone())
    }

    pub fn transaction<T, E: From<RepositoryError>>(
        &self,
        operation: impl FnOnce(&mut CellSqlExecutor) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut operation = Some(operation);
        let mut result = None;
        let mut callback = || -> Result<(), JsValue> {
            let Some(operation) = operation.take() else {
                return Err(JsValue::from_str("cell transaction callback invoked twice"));
            };
            result = Some(operation(&mut self.executor()));
            match result.as_ref() {
                Some(Ok(_)) => Ok(()),
                _ => Err(JsValue::from_str("cell SQL transaction failed")),
            }
        };
        // Any Rust error is thrown through the JS callback, causing rollback.
        // Nothing outside this callback is marked committed before call1 returns.
        let closure =
            ScopedClosure::<dyn FnMut() -> Result<(), JsValue>>::borrow_mut_assert_unwind_safe(
                &mut callback,
            );
        let committed = self.transaction_sync.call1(&self.storage, closure.as_ref());
        drop(closure);
        match (committed, result) {
            (_, Some(Err(error))) => Err(error),
            (Err(error), _) => Err(js_error(error).into()),
            (Ok(_), Some(Ok(value))) => Ok(value),
            (Ok(_), None) => {
                Err(RepositoryError::Model("cell transaction callback did not run".into()).into())
            }
        }
    }
}

/// Shared SQL operations are async for SQLx, but every call through the cell
/// executor completes synchronously. Refuse accidental async work; never spin
/// or let a pending future outlive the transaction callback.
pub(crate) fn finish_sql<T, E: From<RepositoryError>>(
    future: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => Err(RepositoryError::Model(
            "cell SQL transaction attempted to await asynchronous work".into(),
        )
        .into()),
    }
}

#[derive(Clone)]
pub(crate) struct CellSqlExecutor(SqlStorage);
pub(crate) struct CellSqlRow(HashMap<String, SqlStorageValue>);

impl CellSqlRow {
    fn value(&self, column: &str) -> Result<&SqlStorageValue, RepositoryError> {
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
            SqlStorageValue::String(value) => Ok(value.clone()),
            _ => Err(Self::invalid(column)),
        }
    }
    fn integer(&self, column: &'static str) -> Result<i64, RepositoryError> {
        match self.value(column)? {
            SqlStorageValue::Integer(value) => Ok(*value),
            _ => Err(Self::invalid(column)),
        }
    }
    fn optional_text(&self, column: &'static str) -> Result<Option<String>, RepositoryError> {
        match self.value(column)? {
            SqlStorageValue::Null => Ok(None),
            _ => self.text(column).map(Some),
        }
    }
    fn optional_timestamp(
        &self,
        column: &'static str,
    ) -> Result<Option<SystemTime>, RepositoryError> {
        match self.value(column)? {
            SqlStorageValue::Null => Ok(None),
            _ => self.timestamp(column).map(Some),
        }
    }
    fn optional_integer(&self, column: &'static str) -> Result<Option<i64>, RepositoryError> {
        match self.value(column)? {
            SqlStorageValue::Null => Ok(None),
            _ => self.integer(column).map(Some),
        }
    }
    fn bytes(&self, column: &'static str) -> Result<Vec<u8>, RepositoryError> {
        match self.value(column)? {
            SqlStorageValue::Blob(value) => Ok(value.clone()),
            _ => Err(Self::invalid(column)),
        }
    }
    fn timestamp(&self, column: &'static str) -> Result<SystemTime, RepositoryError> {
        match self.value(column)? {
            SqlStorageValue::String(value) => sqlite_codec::decode(value),
            SqlStorageValue::Float(value) => sqlite_codec::decode_epoch(*value),
            SqlStorageValue::Integer(value) => sqlite_codec::decode_epoch(*value as f64),
            _ => Err(Self::invalid(column)),
        }
    }
}

impl CellSqlExecutor {
    fn run(&self, statement: Statement<'_>) -> Result<worker::SqlCursor, RepositoryError> {
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
                    bindings.push(SqlStorageValue::String(sqlite_codec::encode(value)?));
                }
                SqlPart::LedgerNow | SqlPart::LedgerNowEpoch => {
                    sql.push_str("unixepoch('now','subsec')")
                }
                SqlPart::LedgerDeadline(duration) => {
                    sql.push_str("(unixepoch('now','subsec') + ?)");
                    bindings.push(SqlStorageValue::Float(duration.as_secs_f64()));
                }
                SqlPart::LedgerDeadlineIsLive(deadline) => {
                    sql.push_str("CAST(? AS REAL) > unixepoch('now','subsec')");
                    bindings.push(SqlStorageValue::String(sqlite_codec::encode(deadline)?));
                }
                SqlPart::LedgerJson(json) => {
                    sql.push('?');
                    bindings.push(SqlStorageValue::String(json));
                }
                SqlPart::Sql(text) => sql.push_str(&text),
                SqlPart::Bind(value) => {
                    sql.push('?');
                    bindings.push(match value {
                        SqlBind::Text(value) | SqlBind::Metadata(value) => {
                            SqlStorageValue::String(value)
                        }
                        SqlBind::Bytes(value) => SqlStorageValue::Blob(value.into_owned()),
                        // Reject values outside JS's exact range, never round a
                        // sequence/fence silently while crossing the binding.
                        SqlBind::Integer(value) => {
                            SqlStorageValue::try_from_i64(value).map_err(|error| {
                                RepositoryError::Model(format!(
                                    "cell SQL integer is not exactly representable: {error}"
                                ))
                            })?
                        }
                        SqlBind::Timestamp(value) => {
                            SqlStorageValue::String(sqlite_codec::encode(value)?)
                        }
                    });
                }
            }
        }
        self.0.exec(&sql, Some(bindings)).map_err(worker_error)
    }
}

impl SqlExecutor for CellSqlExecutor {
    type Row = CellSqlRow;
    const EVENT_SELECT: &'static str = sqlite_codec::EVENT_SELECT;
    const SNAPSHOT_SELECT: &'static str = sqlite_codec::SNAPSHOT_SELECT;
    const OUTBOX_SELECT: &'static str = sqlite_codec::OUTBOX_SELECT;
    const NOW: &'static str = "CURRENT_TIMESTAMP";
    const COMMAND_LEDGER_SELECT: &'static str = sqlite_codec::COMMAND_LEDGER_SELECT;
    const COMMAND_LEDGER_LOCK_SUFFIX: &'static str = "";
    const COMMAND_LEDGER_COMPACTION_LOCK_SUFFIX: &'static str = "";

    async fn query(&mut self, statement: Statement<'_>) -> Result<Vec<Self::Row>, RepositoryError> {
        let cursor = self.run(statement)?;
        let names = cursor.column_names();
        cursor
            .raw()
            .map(|row| {
                let values = row.map_err(worker_error)?;
                if values.len() != names.len() {
                    return Err(RepositoryError::Model(
                        "cell SQL row width differs from its columns".into(),
                    ));
                }
                Ok(CellSqlRow(names.iter().cloned().zip(values).collect()))
            })
            .collect()
    }

    async fn execute(&mut self, statement: Statement<'_>) -> Result<u64, RepositoryError> {
        Ok(self.run(statement)?.rows_written() as u64)
    }
}

fn js_error(error: JsValue) -> RepositoryError {
    worker_error(worker::Error::from(error))
}
fn worker_error(error: worker::Error) -> RepositoryError {
    // The SDK does not expose structured SQLite error codes. Unknown runtime
    // failures must not permanently discard a command or pending delivery.
    // Deterministic binding/row validation errors are classified above instead.
    RepositoryError::retryable_storage("cell SQL", error)
}
