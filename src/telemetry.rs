//! Internal telemetry vocabulary shared by metrics and tracing paths.
//!
//! The constants in this module are framework-owned labels, label values, and
//! span names. They intentionally avoid request ids, trace ids, payload fields,
//! aggregate ids, user ids, and other high-cardinality data.
#![allow(clippy::items_after_test_module)]

use crate::bus::FailureAction;

#[cfg(feature = "otel")]
use crate::bus::Message;
#[cfg(feature = "metrics")]
use crate::bus::TransportErrorKind;
#[cfg(feature = "metrics")]
use crate::microsvc::HandlerError;

#[cfg(feature = "metrics")]
pub(crate) mod metric_names {
    pub(crate) const SERVICE_INFO: &str = "distributed_service_info";
    pub(crate) const MICROSVC_DISPATCH_TOTAL: &str = "distributed_microsvc_dispatch_total";
    pub(crate) const MICROSVC_DISPATCH_DURATION_SECONDS: &str =
        "distributed_microsvc_dispatch_duration_seconds";
    pub(crate) const TRANSPORT_MESSAGES_TOTAL: &str = "distributed_transport_messages_total";
    pub(crate) const TRANSPORT_FAILURES_TOTAL: &str = "distributed_transport_failures_total";
    pub(crate) const OUTBOX_MESSAGES_TOTAL: &str = "distributed_outbox_messages_total";
    pub(crate) const OUTBOX_PENDING_MESSAGES: &str = "distributed_outbox_pending_messages";
    pub(crate) const OUTBOX_OLDEST_PENDING_AGE_SECONDS: &str =
        "distributed_outbox_oldest_pending_age_seconds";
    pub(crate) const GRAPHQL_REQUEST_TOTAL: &str = "distributed_graphql_request_total";
    pub(crate) const GRAPHQL_REQUEST_DURATION_SECONDS: &str =
        "distributed_graphql_request_duration_seconds";
}

#[cfg(feature = "metrics")]
pub(crate) mod metric_labels {
    pub(crate) const SERVICE: &str = "service";
    pub(crate) const VERSION: &str = "version";
    pub(crate) const MESSAGE_KIND: &str = "message_kind";
    pub(crate) const MESSAGE: &str = "message";
    pub(crate) const STATUS: &str = "status";
    pub(crate) const TRANSPORT: &str = "transport";
    pub(crate) const OUTCOME: &str = "outcome";
    pub(crate) const FAILURE_CLASS: &str = "failure_class";
    pub(crate) const ACTION: &str = "action";
    pub(crate) const LE: &str = "le";
}

#[cfg(feature = "metrics")]
pub(crate) mod metric_values {
    pub(crate) const UNNAMED_SERVICE: &str = "unnamed";
    pub(crate) const UNKNOWN_MESSAGE: &str = "unknown";
}

#[cfg(feature = "metrics")]
pub(crate) mod dispatch_status {
    pub(crate) const SUCCESS: &str = "success";
    pub(crate) const UNKNOWN_COMMAND: &str = "unknown_command";
    pub(crate) const DECODE_FAILED: &str = "decode_failed";
    pub(crate) const REJECTED: &str = "rejected";
    pub(crate) const NOT_FOUND: &str = "not_found";
    pub(crate) const UNAUTHORIZED: &str = "unauthorized";
    pub(crate) const REPOSITORY_ERROR: &str = "repository_error";
    pub(crate) const GUARD_REJECTED: &str = "guard_rejected";
    pub(crate) const OTHER_ERROR: &str = "other_error";
}

pub(crate) mod transport_outcome {
    pub(crate) const ACK: &str = "ack";
    pub(crate) const NACK: &str = "nack";
    pub(crate) const DEAD_LETTER: &str = "dead_letter";
    pub(crate) const PARK: &str = "park";
    pub(crate) const IGNORED: &str = "ignored";
    pub(crate) const LOG_AND_ACK: &str = "log_and_ack";
}

#[cfg(feature = "metrics")]
pub(crate) mod failure_class {
    pub(crate) const RETRYABLE: &str = "retryable";
    pub(crate) const PERMANENT: &str = "permanent";
}

pub(crate) mod failure_action {
    pub(crate) const NACK: &str = "nack";
    pub(crate) const DEAD_LETTER: &str = "dead_letter";
    pub(crate) const PARK: &str = "park";
    pub(crate) const LOG_AND_ACK: &str = "log_and_ack";
    pub(crate) const STOP: &str = "stop";
    pub(crate) const RECV_ERROR: &str = "recv_error";
    pub(crate) const SETTLE_ACK: &str = "settle_ack";
    pub(crate) const SETTLE_NACK: &str = "settle_nack";
    pub(crate) const SETTLE_DEAD_LETTER: &str = "settle_dead_letter";
    pub(crate) const SETTLE_PARK: &str = "settle_park";
    pub(crate) const SETTLE_ERROR: &str = "settle_error";
}

#[cfg(feature = "metrics")]
pub(crate) mod outbox_outcome {
    pub(crate) const PUBLISHED: &str = "published";
    pub(crate) const RELEASED: &str = "released";
    pub(crate) const FAILED: &str = "failed";
}

#[cfg(test)]
#[cfg(feature = "metrics")]
pub(crate) mod privacy_policy {
    pub(crate) const ALLOWED_METRIC_LABELS: &[&str] = &[
        super::metric_labels::SERVICE,
        super::metric_labels::VERSION,
        super::metric_labels::MESSAGE_KIND,
        super::metric_labels::MESSAGE,
        super::metric_labels::STATUS,
        super::metric_labels::TRANSPORT,
        super::metric_labels::OUTCOME,
        super::metric_labels::FAILURE_CLASS,
        super::metric_labels::ACTION,
        super::metric_labels::LE,
        // GraphQL query engine (specs/query-service-graphql).
        "root_field",
    ];

    pub(crate) const FORBIDDEN_METRIC_LABELS: &[&str] = &[
        "request_id",
        "trace_id",
        "span_id",
        "correlation_id",
        "causation_id",
        "message_id",
        "aggregate_id",
        "aggregate_type",
        "stream_id",
        "user_id",
        "tenant_id",
        "payload",
        "metadata",
        "http_path",
        "http_target",
        "http_route",
        "http_user_agent",
    ];
}

#[cfg(feature = "metrics")]
pub(crate) fn service_label(service: Option<&str>) -> String {
    service
        .filter(|value| !value.is_empty())
        .unwrap_or(metric_values::UNNAMED_SERVICE)
        .to_string()
}

#[cfg(feature = "metrics")]
pub(crate) fn handler_error_status(error: &HandlerError) -> &'static str {
    match error {
        HandlerError::UnknownCommand(_) => dispatch_status::UNKNOWN_COMMAND,
        HandlerError::DecodeFailed(_) => dispatch_status::DECODE_FAILED,
        HandlerError::Rejected(_) => dispatch_status::REJECTED,
        HandlerError::NotFound(_) => dispatch_status::NOT_FOUND,
        HandlerError::Unauthorized(_) => dispatch_status::UNAUTHORIZED,
        HandlerError::Repository(_)
        | HandlerError::Projection(_)
        | HandlerError::UnqualifiedProjectionDelivery(_)
        | HandlerError::ProjectionRepairPending { .. }
        | HandlerError::ProjectionTerminalRecorded { .. }
        | HandlerError::ProjectionDeliveryHalted { .. } => dispatch_status::REPOSITORY_ERROR,
        HandlerError::GuardRejected(_) => dispatch_status::GUARD_REJECTED,
        HandlerError::ApplicationReloading | HandlerError::Other(_) => dispatch_status::OTHER_ERROR,
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn handler_message_label<'a>(message: &'a str, error: Option<&HandlerError>) -> &'a str {
    if matches!(error, Some(HandlerError::UnknownCommand(_))) {
        metric_values::UNKNOWN_MESSAGE
    } else {
        message
    }
}

#[cfg(feature = "metrics")]
pub(crate) fn transport_failure_class(kind: TransportErrorKind) -> &'static str {
    match kind {
        TransportErrorKind::Retryable => failure_class::RETRYABLE,
        TransportErrorKind::Permanent => failure_class::PERMANENT,
    }
}

pub(crate) fn failure_action_label(action: FailureAction) -> &'static str {
    match action {
        FailureAction::Nack => failure_action::NACK,
        FailureAction::DeadLetter => failure_action::DEAD_LETTER,
        FailureAction::Park => failure_action::PARK,
        FailureAction::LogAndAck => failure_action::LOG_AND_ACK,
        FailureAction::Stop => failure_action::STOP,
    }
}

pub(crate) fn settle_failure_action(settle_action: &'static str) -> &'static str {
    match settle_action {
        transport_outcome::ACK => failure_action::SETTLE_ACK,
        transport_outcome::NACK => failure_action::SETTLE_NACK,
        transport_outcome::DEAD_LETTER => failure_action::SETTLE_DEAD_LETTER,
        transport_outcome::PARK => failure_action::SETTLE_PARK,
        _ => failure_action::SETTLE_ERROR,
    }
}

/// Build framework message spans with the stable span attributes Distributed
/// owns today:
///
/// - `distributed.message.name`
/// - `distributed.message.kind`
/// - `messaging.message.id`
#[cfg(feature = "otel")]
macro_rules! framework_message_span {
    ($name:literal, $message:expr) => {
        tracing::info_span!(
            $name,
            distributed.message.name = %$message.name(),
            distributed.message.kind = %$message.kind.as_str(),
            messaging.message.id = %$message.id().unwrap_or("")
        )
    };
}

#[cfg(feature = "otel")]
pub(crate) fn microsvc_dispatch_span(message: &Message) -> tracing::Span {
    framework_message_span!("distributed.microsvc.dispatch", message)
}

#[cfg(feature = "otel")]
pub(crate) fn microsvc_handler_span(message: &Message) -> tracing::Span {
    framework_message_span!("distributed.handler", message)
}

#[cfg(feature = "otel")]
pub(crate) fn transport_receive_span(message: &Message) -> tracing::Span {
    framework_message_span!("distributed.transport.receive", message)
}

#[cfg(feature = "otel")]
pub(crate) fn outbox_publish_span(message: &Message) -> tracing::Span {
    framework_message_span!("distributed.outbox.publish", message)
}
