use std::fmt;

use crate::repository::RepositoryError;
use crate::EventRecordError;

/// Saga failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum SagaError {
    /// The saga stream does not exist (message for an unknown saga).
    NotFound { saga_id: String },
    /// `handle` on a saga that was never started.
    NotStarted,
    /// The definition produced an invalid step/command.
    Definition(String),
    /// A message lacked or had malformed saga metadata.
    Malformed(String),
    /// Event encode/decode failure.
    Event(EventRecordError),
    /// Persistence failure (including `ConcurrentWrite` after retries).
    Repository(RepositoryError),
}

impl fmt::Display for SagaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SagaError::NotFound { saga_id } => write!(f, "saga `{saga_id}` not found"),
            SagaError::NotStarted => write!(f, "saga has not been started"),
            SagaError::Definition(message) => write!(f, "saga definition error: {message}"),
            SagaError::Malformed(message) => write!(f, "malformed saga message: {message}"),
            SagaError::Event(err) => write!(f, "saga event error: {}", err.message),
            SagaError::Repository(err) => write!(f, "saga repository error: {err}"),
        }
    }
}

impl std::error::Error for SagaError {}

impl From<EventRecordError> for SagaError {
    fn from(err: EventRecordError) -> Self {
        SagaError::Event(err)
    }
}

impl From<RepositoryError> for SagaError {
    fn from(err: RepositoryError) -> Self {
        SagaError::Repository(err)
    }
}
