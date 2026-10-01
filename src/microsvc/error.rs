//! Error types for microsvc command handlers.

use std::error::Error;
use std::fmt;

use super::projector::ProjectionRepairHandle;
use crate::bus::{PayloadDecodeError, TransportError, TransportErrorKind};
use crate::lock::RetryClass;
use crate::projection_protocol::ProjectionProtocolError;
use crate::table::TableStoreError;
use crate::{repository::RepositoryError, EventRecordError};

/// Error type for command handler operations.
#[derive(Debug)]
#[non_exhaustive]
pub enum HandlerError {
    /// Supervisor generation admission is temporarily closed. Retain the exact
    /// delivery and stop the receive loop until the host retries after reload.
    ApplicationReloading,
    /// No handler registered for this command name.
    UnknownCommand(String),
    /// Payload decode / deserialization failed.
    DecodeFailed(String),
    /// Business logic rejected the command (validation, invariant violation).
    Rejected(String),
    /// Aggregate or resource not found.
    NotFound(String),
    /// Missing or invalid authentication / authorization.
    Unauthorized(String),
    /// Repository error (event store, snapshots, read models).
    Repository(RepositoryError),
    /// Guard rejected the command (input validation failed).
    GuardRejected(String),
    /// Causal projection protocol/storage failure.
    Projection(ProjectionProtocolError),
    /// A causal projector route was reached without adapter-authenticated
    /// ordered delivery or another required stable envelope identity.
    UnqualifiedProjectionDelivery(String),
    /// An explicitly repaired partition is waiting for its exact failed input;
    /// unrelated delivery must remain retryable and cannot invoke the handler.
    ProjectionRepairPending { failure_id: String },
    /// The terminal failure is already durable. The transport must retain this
    /// exact delivery and stop so an operator can repair then restart.
    ProjectionTerminalRecorded { repair: ProjectionRepairHandle },
    /// A permanent causal-projector failure could not be represented by a
    /// durable terminal record. The transport must retain this exact delivery
    /// and stop rather than applying an ordinary dead-letter/drop policy across
    /// an unproven causal gap.
    ProjectionDeliveryHalted { source: Box<HandlerError> },
    /// Other error.
    Other(Box<dyn Error + Send + Sync>),
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerError::ApplicationReloading => f.write_str("application generation is reloading"),
            HandlerError::UnknownCommand(name) => write!(f, "unknown command: {}", name),
            HandlerError::DecodeFailed(msg) => write!(f, "decode failed: {}", msg),
            HandlerError::Rejected(msg) => write!(f, "rejected: {}", msg),
            HandlerError::NotFound(id) => write!(f, "not found: {}", id),
            HandlerError::Unauthorized(msg) => write!(f, "unauthorized: {}", msg),
            HandlerError::Repository(e) => write!(f, "repository error: {}", e),
            HandlerError::GuardRejected(name) => {
                write!(f, "guard rejected command: {}", name)
            }
            HandlerError::Projection(error) => write!(f, "projection error: {error}"),
            HandlerError::UnqualifiedProjectionDelivery(message) => {
                write!(f, "unqualified causal projector delivery: {message}")
            }
            HandlerError::ProjectionRepairPending { failure_id } => write!(
                f,
                "projection repair is waiting for exact failed input `{failure_id}`"
            ),
            HandlerError::ProjectionTerminalRecorded { repair } => write!(
                f,
                "projection terminal failure `{}` was recorded; retain delivery and stop; repair handle: {repair}",
                repair.failure_id()
            ),
            HandlerError::ProjectionDeliveryHalted { .. } => f.write_str(
                "causal projector delivery halted before a durable terminal record was available; retain delivery and stop",
            ),
            HandlerError::Other(e) => write!(f, "handler error: {}", e),
        }
    }
}

impl Error for HandlerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            HandlerError::Repository(e) => Some(e),
            HandlerError::Projection(e) => Some(e),
            HandlerError::ProjectionDeliveryHalted { source } => Some(source.as_ref()),
            HandlerError::Other(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<RepositoryError> for HandlerError {
    fn from(err: RepositoryError) -> Self {
        HandlerError::Repository(err)
    }
}

impl From<EventRecordError> for HandlerError {
    fn from(err: EventRecordError) -> Self {
        HandlerError::Other(Box::new(err))
    }
}

impl From<ProjectionProtocolError> for HandlerError {
    fn from(error: ProjectionProtocolError) -> Self {
        Self::Projection(error)
    }
}

impl From<TableStoreError> for HandlerError {
    fn from(error: TableStoreError) -> Self {
        Self::Projection(error.into())
    }
}

impl From<serde_json::Error> for HandlerError {
    fn from(err: serde_json::Error) -> Self {
        HandlerError::DecodeFailed(err.to_string())
    }
}

impl From<PayloadDecodeError> for HandlerError {
    fn from(err: PayloadDecodeError) -> Self {
        HandlerError::DecodeFailed(err.0)
    }
}

impl HandlerError {
    /// Return the opaque operator repair handle carried by a durably recorded
    /// terminal projector failure.
    pub fn projection_repair_handle(&self) -> Option<&ProjectionRepairHandle> {
        match self {
            Self::ProjectionTerminalRecorded { repair } => Some(repair),
            _ => None,
        }
    }

    /// Map this error to an HTTP-style status code.
    pub fn status_code(&self) -> u16 {
        match self {
            HandlerError::ApplicationReloading => 503,
            HandlerError::UnknownCommand(_) => 404,
            HandlerError::DecodeFailed(_) => 400,
            HandlerError::Rejected(_) => 422,
            HandlerError::NotFound(_) => 404,
            HandlerError::Unauthorized(_) => 401,
            HandlerError::Repository(_) => 500,
            HandlerError::GuardRejected(_) => 400,
            HandlerError::Projection(_)
            | HandlerError::UnqualifiedProjectionDelivery(_)
            | HandlerError::ProjectionRepairPending { .. }
            | HandlerError::ProjectionTerminalRecorded { .. }
            | HandlerError::ProjectionDeliveryHalted { .. } => 500,
            HandlerError::Other(_) => 500,
        }
    }

    /// Message safe to return to an untrusted client across a transport.
    ///
    /// Server-internal failures (status 5xx — repository/driver/other errors)
    /// are masked to a generic string so SQL text, driver detail, or internal
    /// paths never leak to callers. Client-fault errors (4xx — unknown command,
    /// decode, rejection, auth, guard) keep their descriptive message because
    /// the caller caused them and the detail helps them correct the request.
    ///
    /// The HTTP, gRPC, and Knative ingresses route error bodies through this so
    /// the masking policy lives in exactly one place.
    pub fn client_facing_message(&self) -> String {
        if self.status_code() >= 500 {
            "Internal server error".to_string()
        } else {
            self.to_string()
        }
    }

    /// Classify this error for transport retry purposes (retryable vs permanent).
    ///
    /// A repository error is no longer one blanket "retryable" bucket: it defers
    /// to [`RepositoryError::kind`], so a transient storage outage (connection
    /// refused, pool timeout, `SQLITE_BUSY`) stays retryable while a deterministic
    /// model/read-model/decode fault is permanent — otherwise the latter would be
    /// redelivered forever. `NotFound` and `Other` remain retryable: in an
    /// at-least-once system a not-found is usually an out-of-order delivery race a
    /// later redelivery resolves, and an unclassified error is more often
    /// transient infrastructure than a deterministic bug. Deterministic failures
    /// (unknown routing, decode, rejection, auth, guard) are permanent —
    /// redelivering the identical message cannot change them.
    pub(crate) fn transport_error_kind(&self) -> TransportErrorKind {
        match self {
            HandlerError::Repository(err) => match err.kind() {
                RetryClass::Retryable => TransportErrorKind::Retryable,
                RetryClass::Permanent => TransportErrorKind::Permanent,
            },
            HandlerError::Projection(error) => {
                if super::projector::projection_error_is_retryable(error) {
                    TransportErrorKind::Retryable
                } else {
                    TransportErrorKind::Permanent
                }
            }
            HandlerError::ApplicationReloading
            | HandlerError::ProjectionRepairPending { .. }
            | HandlerError::NotFound(_)
            | HandlerError::Other(_) => TransportErrorKind::Retryable,
            HandlerError::ProjectionTerminalRecorded { .. }
            | HandlerError::ProjectionDeliveryHalted { .. } => TransportErrorKind::Permanent,
            HandlerError::UnknownCommand(_)
            | HandlerError::DecodeFailed(_)
            | HandlerError::Rejected(_)
            | HandlerError::Unauthorized(_)
            | HandlerError::GuardRejected(_)
            | HandlerError::UnqualifiedProjectionDelivery(_) => TransportErrorKind::Permanent,
        }
    }

    pub(crate) fn is_projection_retryable(&self) -> bool {
        self.transport_error_kind().is_retryable()
    }
}

/// Classify and convert a handler error into the transport's retryable/permanent
/// vocabulary. This lives on the microsvc side (not the bus) so the bus core does
/// not depend on `HandlerError`.
impl From<HandlerError> for TransportError {
    fn from(error: HandlerError) -> Self {
        let kind = error.transport_error_kind();
        let retain_and_stop = matches!(
            error,
            HandlerError::ApplicationReloading
                | HandlerError::ProjectionTerminalRecorded { .. }
                | HandlerError::ProjectionDeliveryHalted { .. }
        );
        let transport = TransportError::new(kind, error.to_string()).with_source(error);
        if retain_and_stop {
            transport.retain_and_stop()
        } else {
            transport
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_reloading_retains_delivery_without_reclassifying_business_errors() {
        let reload = TransportError::from(HandlerError::ApplicationReloading);
        assert!(reload.is_retryable());
        assert!(reload.should_retain_and_stop());
        let rejection = TransportError::from(HandlerError::Rejected("invalid slug".into()));
        assert!(rejection.is_permanent());
        assert!(!rejection.should_retain_and_stop());
    }

    #[test]
    fn transient_handler_errors_are_retryable() {
        for error in [
            HandlerError::Repository(RepositoryError::NotFound { id: "agg-1".into() }),
            HandlerError::NotFound("agg-1".into()),
            HandlerError::Other("boom".into()),
        ] {
            assert_eq!(error.transport_error_kind(), TransportErrorKind::Retryable);
        }
    }

    #[test]
    fn deterministic_handler_errors_are_permanent() {
        for error in [
            HandlerError::UnknownCommand("x".into()),
            HandlerError::DecodeFailed("x".into()),
            HandlerError::Rejected("x".into()),
            HandlerError::Unauthorized("x".into()),
            HandlerError::GuardRejected("x".into()),
        ] {
            assert_eq!(error.transport_error_kind(), TransportErrorKind::Permanent);
        }
    }

    /// A deterministic repository fault (a model error, a permanent storage
    /// failure such as a constraint violation) must NOT be classified retryable,
    /// or the runner would redeliver the identical message forever.
    #[test]
    fn deterministic_repository_errors_are_permanent() {
        let io = std::io::Error::new(std::io::ErrorKind::InvalidData, "bad row");
        for error in [
            HandlerError::Repository(RepositoryError::Model("invalid model state".into())),
            HandlerError::Repository(RepositoryError::Replay("version mismatch".into())),
            HandlerError::Repository(RepositoryError::permanent_storage("insert event", io)),
        ] {
            assert_eq!(
                error.transport_error_kind(),
                TransportErrorKind::Permanent,
                "{error}"
            );
        }
    }

    /// A transient storage failure (connection refused, pool timeout, busy) must
    /// stay retryable so a recovered backend gets the redelivery.
    #[test]
    fn transient_repository_storage_errors_are_retryable() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let error = HandlerError::Repository(RepositoryError::retryable_storage("load stream", io));
        assert_eq!(error.transport_error_kind(), TransportErrorKind::Retryable);

        // A concurrency conflict is retryable: another writer won the race.
        let conflict = HandlerError::Repository(RepositoryError::ConcurrentWrite {
            id: "agg-1".into(),
            expected: 1,
            actual: 2,
        });
        assert_eq!(
            conflict.transport_error_kind(),
            TransportErrorKind::Retryable
        );
    }

    #[test]
    fn from_handler_error_preserves_classification_and_source() {
        let err: TransportError = HandlerError::Rejected("invalid".into()).into();
        assert!(err.is_permanent());
        assert!(err.source().is_some());

        let err: TransportError =
            HandlerError::Other(Box::<dyn Error + Send + Sync>::from("infra")).into();
        assert!(err.is_retryable());
    }
}
