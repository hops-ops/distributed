use std::error::Error;
use std::fmt;

use crate::lock::{LockError, RetryClass};
use crate::table::TableStoreError;
use crate::EventRecordError;

/// Load-run-commit attempts before a persistent
/// [`RepositoryError::ConcurrentWrite`] is surfaced.
pub(crate) const CONCURRENT_WRITE_RETRIES: usize = 3;

#[derive(Debug)]
#[non_exhaustive]
pub enum RepositoryError {
    LockPoisoned(&'static str),
    Lock(LockError),
    ConcurrentWrite {
        id: String,
        expected: u64,
        actual: u64,
    },
    DuplicateStreamInBatch {
        id: String,
    },
    DuplicateOutboxMessageInBatch {
        id: String,
    },
    /// A consumer inbox receipt `(consumer, message_id)` was already recorded.
    /// The commit is rolled back so the consumer's effects are not double-applied;
    /// the message has already been processed (an at-least-once replay).
    DuplicateInboxReceipt {
        consumer: String,
        message_id: String,
    },
    /// A consumer inbox receipt had an empty `consumer` or `message_id`. Rejected
    /// uniformly across backends before any write (the relational `CHECK`
    /// constraints are a defense-in-depth backstop).
    InvalidInboxReceipt {
        consumer: String,
        message_id: String,
    },
    /// A raw/legacy repository batch targeted a causal-owned read-model table.
    CausalWriteRequired {
        table: String,
    },
    InvalidStreamIdentity {
        aggregate_type: String,
        aggregate_id: String,
        reason: String,
    },
    NotFound {
        id: String,
    },
    InvalidState {
        id: String,
        expected: &'static str,
        actual: String,
    },
    Replay(String),
    Model(String),
    /// A storage backend (event store, read model, snapshot store) failed to
    /// complete an operation. Unlike [`RepositoryError::Model`] — a deterministic
    /// modeling/decoding fault — this carries an explicit retry classification so
    /// callers can distinguish a transient outage (connection refused, pool
    /// timeout, `SQLITE_BUSY`) from a deterministic failure (constraint
    /// violation, malformed row) without string-sniffing the message.
    ///
    /// The optional `source` preserves the underlying error for diagnostics and
    /// dead-letter metadata; it is exposed through [`Error::source`].
    Storage {
        /// The operation that failed (e.g. `"sqlite insert event"`).
        operation: String,
        /// Whether retrying the same operation may succeed.
        retryable: bool,
        /// The underlying backend error, if available.
        source: Option<Box<dyn Error + Send + Sync>>,
    },
}

impl RepositoryError {
    /// Construct a retryable storage failure carrying its source.
    pub fn retryable_storage(
        operation: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        RepositoryError::Storage {
            operation: operation.into(),
            retryable: true,
            source: Some(Box::new(source)),
        }
    }

    /// Construct a permanent storage failure carrying its source.
    pub fn permanent_storage(
        operation: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        RepositoryError::Storage {
            operation: operation.into(),
            retryable: false,
            source: Some(Box::new(source)),
        }
    }

    /// Classify this error for retry purposes.
    ///
    /// The contract a runner relies on: a retryable error should be redelivered
    /// (a later attempt may succeed); a permanent error should not, because
    /// re-running the identical operation cannot change a deterministic outcome.
    ///
    /// - `Storage { retryable, .. }` reports the classification captured when the
    ///   backend error was mapped (connection/pool/timeout → retryable;
    ///   constraint/decode → permanent).
    /// - `Lock` defers to [`LockError::kind`].
    /// - `ConcurrentWrite` is **retryable**: an optimistic-concurrency conflict
    ///   means another writer won the race; reloading and reapplying typically
    ///   succeeds. This preserves the prior behavior where it fell into the
    ///   retryable bucket.
    /// - `NotFound` is retryable: under at-least-once delivery it is usually an
    ///   out-of-order race a later redelivery resolves.
    /// - The deterministic faults (`Model`, `Replay`, invalid identity/receipt,
    ///   `InvalidState`, duplicate-in-batch, `LockPoisoned`) are permanent.
    pub fn kind(&self) -> RetryClass {
        match self {
            RepositoryError::Storage { retryable, .. } => {
                if *retryable {
                    RetryClass::Retryable
                } else {
                    RetryClass::Permanent
                }
            }
            RepositoryError::Lock(err) => err.kind(),
            RepositoryError::ConcurrentWrite { .. } | RepositoryError::NotFound { .. } => {
                RetryClass::Retryable
            }
            RepositoryError::LockPoisoned(_)
            | RepositoryError::DuplicateStreamInBatch { .. }
            | RepositoryError::DuplicateOutboxMessageInBatch { .. }
            | RepositoryError::DuplicateInboxReceipt { .. }
            | RepositoryError::InvalidInboxReceipt { .. }
            | RepositoryError::CausalWriteRequired { .. }
            | RepositoryError::InvalidStreamIdentity { .. }
            | RepositoryError::InvalidState { .. }
            | RepositoryError::Replay(_)
            | RepositoryError::Model(_) => RetryClass::Permanent,
        }
    }

    /// Whether this error is retryable.
    pub fn is_retryable(&self) -> bool {
        self.kind().is_retryable()
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RepositoryError::LockPoisoned(operation) => {
                write!(f, "repository lock poisoned during {}", operation)
            }
            RepositoryError::Lock(err) => write!(f, "repository lock error: {}", err),
            RepositoryError::ConcurrentWrite {
                id,
                expected,
                actual,
            } => write!(
                f,
                "concurrent write detected for entity {} (expected version {}, got {})",
                id, expected, actual
            ),
            RepositoryError::DuplicateStreamInBatch { id } => {
                write!(f, "duplicate stream id in commit batch: {}", id)
            }
            RepositoryError::DuplicateOutboxMessageInBatch { id } => {
                write!(f, "duplicate outbox message id in commit batch: {}", id)
            }
            RepositoryError::DuplicateInboxReceipt {
                consumer,
                message_id,
            } => write!(
                f,
                "consumer inbox receipt already recorded for consumer `{}`, message `{}`",
                consumer, message_id
            ),
            RepositoryError::InvalidInboxReceipt {
                consumer,
                message_id,
            } => write!(
                f,
                "invalid consumer inbox receipt (consumer `{}`, message `{}`): consumer and message id must be non-empty",
                consumer, message_id
            ),
            RepositoryError::CausalWriteRequired { table } => write!(
                f,
                "table `{table}` is causal-owned and requires the projection commit path"
            ),
            RepositoryError::InvalidStreamIdentity {
                aggregate_type,
                aggregate_id,
                reason,
            } => write!(
                f,
                "invalid stream identity (type `{}`, id `{}`): {}",
                aggregate_type, aggregate_id, reason
            ),
            RepositoryError::NotFound { id } => write!(f, "entity not found: {}", id),
            RepositoryError::InvalidState {
                id,
                expected,
                actual,
            } => write!(
                f,
                "invalid state for entity {} (expected {}, got {})",
                id, expected, actual
            ),
            RepositoryError::Replay(message) => write!(f, "replay error: {}", message),
            RepositoryError::Model(message) => write!(f, "model error: {}", message),
            RepositoryError::Storage {
                operation,
                retryable,
                source,
            } => {
                let class = if *retryable { "retryable" } else { "permanent" };
                match source {
                    Some(source) => {
                        write!(f, "storage error ({class}) during {operation}: {source}")
                    }
                    None => write!(f, "storage error ({class}) during {operation}"),
                }
            }
        }
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RepositoryError::Lock(err) => Some(err),
            RepositoryError::Storage {
                source: Some(source),
                ..
            } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<LockError> for RepositoryError {
    fn from(err: LockError) -> Self {
        RepositoryError::Lock(err)
    }
}

impl From<TableStoreError> for RepositoryError {
    fn from(err: TableStoreError) -> Self {
        if let TableStoreError::CausalWriteRequired { table } = err {
            return RepositoryError::CausalWriteRequired { table };
        }
        // Map to `Storage` so the read-model error keeps a retry signal and its
        // source instead of collapsing to an opaque `Model` string. Locks and
        // structured backend failures carry the retry classification through;
        // every other read-model variant is deterministic (a concurrency
        // conflict, serde/metadata fault, or not-found will fail the same way on
        // redelivery). Legacy `TableStoreError::Storage` remains string-only and
        // therefore permanent by default; guessing retryability from text would
        // risk an infinite poison-message loop.
        let retryable = match &err {
            TableStoreError::Lock(lock) => lock.is_retryable(),
            TableStoreError::BackendStorage { retryable, .. } => *retryable,
            _ => false,
        };
        RepositoryError::Storage {
            operation: "read model".into(),
            retryable,
            source: Some(Box::new(err)),
        }
    }
}

impl From<EventRecordError> for RepositoryError {
    fn from(err: EventRecordError) -> Self {
        // Event (de)serialization faults are deterministic: the same bytes will
        // fail the same way on redelivery. Classify as permanent storage, but
        // preserve the source for diagnostics rather than stringifying it away.
        RepositoryError::Storage {
            operation: "event record".into(),
            retryable: false,
            source: Some(Box::new(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_backend_retry_class_survives_repository_conversion() {
        let transient = RepositoryError::from(TableStoreError::BackendStorage {
            operation: "sqlite projection write".into(),
            retryable: true,
            message: "database is busy".into(),
        });
        let permanent = RepositoryError::from(TableStoreError::BackendStorage {
            operation: "postgres projection write".into(),
            retryable: false,
            message: "constraint violation".into(),
        });

        assert!(transient.is_retryable());
        assert!(!permanent.is_retryable());
        assert!(transient.to_string().contains("retryable"));
        assert!(permanent.to_string().contains("permanent"));

        let causal = RepositoryError::from(TableStoreError::CausalWriteRequired {
            table: "todo_views".into(),
        });
        assert!(matches!(
            causal,
            RepositoryError::CausalWriteRequired { ref table } if table == "todo_views"
        ));
        assert!(!causal.is_retryable());
    }
}
