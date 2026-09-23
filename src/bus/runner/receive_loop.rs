use std::future::Future;
use std::sync::Arc;

use crate::bus::source::{MessageSource, ReceivedMessage};
use crate::bus::{
    FailureAction, IdlePolicy, MessageRouter, RunOptions, TransportError, TransportErrorKind,
};
use crate::bus::{Message, MessageKind};

/// Run the receive loop for a direct transport source.
///
/// For each message the runner:
///
/// 1. enforces the inbox stable-id contract (a no-op in idempotent mode);
/// 2. dispatches through [`MessageRouter::dispatch`];
/// 3. on success, acknowledges via the adapter;
/// 4. on failure, routes through [`RunOptions::failure_policy`] — retryable
///    failures are nacked for redelivery, permanent failures take the configured
///    action ([dead-letter](FailureAction::DeadLetter), [park](FailureAction::Park),
///    [log-and-ack](FailureAction::LogAndAck), or [stop](FailureAction::Stop)).
///
/// A message with no registered handler is **intentionally ignored**: the runner
/// acks it and moves on rather than dead-lettering it. Fan-out event transports
/// may deliver events this service does not consume, and acking matches
/// `microsvc::subscribe`; production transports should use
/// [`MessageRouter::subscription_plan`] to avoid delivering unrelated messages at all.
///
/// The runner **acks only after handler effects have completed**, never before.
/// It stops gracefully when the source returns `Ok(None)`, having fully settled
/// the in-flight message first. Receive and settle errors are propagated, not
/// swallowed: a returned `Err` ends the run and the supervisor may restart it
/// (already-committed effects make redelivery safe).
///
/// Inbox note: until the consumer-inbox subtask lands, inbox mode enforces the
/// stable-id requirement and then dispatches like idempotent mode. The
/// receipt-commit wrapping that makes it effectively-once is added there.
///
/// `I: Send` keeps the returned future `Send` so the runner can be spawned on a
/// multi-threaded executor regardless of the inbox hook type.
pub async fn run_source<R, S, I>(
    router: Arc<R>,
    mut source: S,
    options: RunOptions<I>,
) -> Result<(), TransportError>
where
    R: MessageRouter,
    S: MessageSource,
    I: Send,
{
    let service = router.consumer_group();
    let transport = source.transport_name();

    loop {
        let Some(received) = recv_next(&mut source, service, transport, options.idle).await? else {
            match options.idle {
                IdlePolicy::Drain => break,
                IdlePolicy::Wait => {
                    source.wait().await?;
                    continue;
                }
            }
        };

        // A delivery the transport could not decode is a permanent failure: it
        // carries no valid message to dispatch, and it must NOT be treated as an
        // empty message (which would route to ack-and-ignore below and silently
        // drop a corrupt row). Route it through the failure policy directly, the
        // same as a permanent dispatch failure, so it is dead-lettered/parked.
        if let Some(error) = received.decode_error() {
            let action = options.failure_policy.resolve(error);
            record_transport_failure(service, transport, error.kind(), action);
            let kind = received.message().kind;
            match action {
                FailureAction::Nack => {
                    let reason = error.to_string();
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::NACK,
                        crate::telemetry::transport_outcome::NACK,
                        || received.nack(&reason),
                    )
                    .await?;
                }
                FailureAction::DeadLetter => {
                    let reason = error.to_string();
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::DEAD_LETTER,
                        crate::telemetry::transport_outcome::DEAD_LETTER,
                        || received.dead_letter(&reason),
                    )
                    .await?;
                }
                FailureAction::Park => {
                    let reason = error.to_string();
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::PARK,
                        crate::telemetry::transport_outcome::PARK,
                        || received.park(&reason),
                    )
                    .await?;
                }
                FailureAction::LogAndAck => {
                    eprintln!("[bus::runner] dropping undecodable message after permanent failure: {error}");
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::ACK,
                        crate::telemetry::transport_outcome::LOG_AND_ACK,
                        || received.ack(),
                    )
                    .await?;
                }
                FailureAction::Stop => return Err(TransportError::permanent(error.to_string())),
            }
            continue;
        }
        // No handler for this message: intentionally ignore (ack) rather than
        // dead-letter, so unrelated fan-out events don't pile into the DLQ.
        if !router.handles(received.message().kind, received.message().name()) {
            let kind = received.message().kind;
            settle_and_record(
                service,
                transport,
                kind,
                crate::telemetry::transport_outcome::ACK,
                crate::telemetry::transport_outcome::IGNORED,
                || received.ack(),
            )
            .await?;
            continue;
        }
        let kind = received.message().kind;
        match dispatch(
            router.as_ref(),
            &options,
            received.message(),
            received.ordered_delivery(),
        )
        .await
        {
            Ok(()) => {
                settle_and_record(
                    service,
                    transport,
                    kind,
                    crate::telemetry::transport_outcome::ACK,
                    crate::telemetry::transport_outcome::ACK,
                    || received.ack(),
                )
                .await?;
            }
            Err(error) if error.should_retain_and_stop() => {
                record_transport_failure(
                    service,
                    transport,
                    error.kind(),
                    crate::telemetry::transport_outcome::NACK,
                );
                let reason = error.to_string();
                settle_and_record(
                    service,
                    transport,
                    kind,
                    crate::telemetry::transport_outcome::NACK,
                    crate::telemetry::transport_outcome::NACK,
                    || received.nack(&reason),
                )
                .await?;
                return Err(error);
            }
            Err(error) => match options.failure_policy.resolve(&error) {
                action @ FailureAction::Nack => {
                    record_transport_failure(service, transport, error.kind(), action);
                    let reason = error.to_string();
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::NACK,
                        crate::telemetry::transport_outcome::NACK,
                        || received.nack(&reason),
                    )
                    .await?;
                }
                action @ FailureAction::DeadLetter => {
                    record_transport_failure(service, transport, error.kind(), action);
                    let reason = error.to_string();
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::DEAD_LETTER,
                        crate::telemetry::transport_outcome::DEAD_LETTER,
                        || received.dead_letter(&reason),
                    )
                    .await?;
                }
                action @ FailureAction::Park => {
                    record_transport_failure(service, transport, error.kind(), action);
                    let reason = error.to_string();
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::PARK,
                        crate::telemetry::transport_outcome::PARK,
                        || received.park(&reason),
                    )
                    .await?;
                }
                FailureAction::LogAndAck => {
                    record_transport_failure(
                        service,
                        transport,
                        error.kind(),
                        FailureAction::LogAndAck,
                    );
                    eprintln!(
                        "[bus::runner] dropping message '{}' after permanent failure: {error}",
                        received.message().name()
                    );
                    settle_and_record(
                        service,
                        transport,
                        kind,
                        crate::telemetry::transport_outcome::ACK,
                        crate::telemetry::transport_outcome::LOG_AND_ACK,
                        || received.ack(),
                    )
                    .await?;
                }
                FailureAction::Stop => {
                    record_transport_failure(service, transport, error.kind(), FailureAction::Stop);
                    return Err(error);
                }
            },
        }
    }
    Ok(())
}

async fn settle_and_record<F, Fut>(
    service: Option<&str>,
    transport: &str,
    kind: MessageKind,
    settle_action: &'static str,
    outcome: &'static str,
    settle: F,
) -> Result<(), TransportError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), TransportError>>,
{
    match settle().await {
        Ok(()) => {
            record_transport_message(service, transport, kind, outcome);
            Ok(())
        }
        Err(error) => {
            record_transport_failure(
                service,
                transport,
                error.kind(),
                crate::telemetry::settle_failure_action(settle_action),
            );
            Err(error)
        }
    }
}

async fn recv_next<S: MessageSource>(
    source: &mut S,
    service: Option<&str>,
    transport: &str,
    idle: IdlePolicy,
) -> Result<Option<S::Received>, TransportError> {
    loop {
        match source.recv().await {
            Ok(received) => return Ok(received),
            Err(error) => {
                record_transport_failure(
                    service,
                    transport,
                    error.kind(),
                    crate::telemetry::failure_action::RECV_ERROR,
                );
                // A long-lived consumer treats a transient recv (SQLite BUSY,
                // brief broker blip) as idle: wait and try again. Drain keeps
                // the historical contract that recv errors end the run.
                if idle == IdlePolicy::Wait && error.kind().is_retryable() {
                    source.wait().await?;
                    continue;
                }
                return Err(error);
            }
        }
    }
}

/// Run consumer execution for one message and classify the outcome.
///
/// Enforces the inbox stable-id contract first (idempotent mode yields no key
/// and skips it), then dispatches. A failed stable-id check is a permanent
/// failure — redelivery cannot supply a missing or malformed id.
async fn dispatch<R: MessageRouter, I>(
    router: &R,
    options: &RunOptions<I>,
    message: &Message,
    ordered: Option<&crate::bus::OrderedDelivery>,
) -> Result<(), TransportError> {
    #[cfg(feature = "otel")]
    {
        use tracing::Instrument as _;

        let span = transport_receive_span(message);
        crate::trace_context::set_span_parent_from_metadata_if_no_current_span(
            &span,
            &message.metadata,
        );
        return async {
            options
                .validate_message_id(message)
                .map_err(|err| TransportError::permanent(err.to_string()).with_source(err))?;
            router.dispatch_ordered(message, ordered).await
        }
        .instrument(span)
        .await;
    }

    #[cfg(not(feature = "otel"))]
    {
        options
            .validate_message_id(message)
            .map_err(|err| TransportError::permanent(err.to_string()).with_source(err))?;
        router.dispatch_ordered(message, ordered).await
    }
}

#[cfg(feature = "otel")]
fn transport_receive_span(message: &Message) -> tracing::Span {
    crate::telemetry::transport_receive_span(message)
}

fn record_transport_message(
    service: Option<&str>,
    transport: &str,
    kind: MessageKind,
    outcome: &str,
) {
    #[cfg(feature = "metrics")]
    crate::metrics::record_transport_message(service, transport, kind, outcome);
    #[cfg(not(feature = "metrics"))]
    let _ = (service, transport, kind, outcome);
}

fn record_transport_failure<A>(
    service: Option<&str>,
    transport: &str,
    kind: TransportErrorKind,
    action: A,
) where
    A: IntoFailureActionLabel,
{
    #[cfg(feature = "metrics")]
    crate::metrics::record_transport_failure(
        service,
        transport,
        crate::telemetry::transport_failure_class(kind),
        action.into_failure_action_label(),
    );
    #[cfg(not(feature = "metrics"))]
    {
        let _ = (service, transport, kind);
        let _ = action.into_failure_action_label();
    }
}

trait IntoFailureActionLabel {
    fn into_failure_action_label(self) -> &'static str;
}

impl IntoFailureActionLabel for FailureAction {
    fn into_failure_action_label(self) -> &'static str {
        crate::telemetry::failure_action_label(self)
    }
}

impl IntoFailureActionLabel for &'static str {
    fn into_failure_action_label(self) -> &'static str {
        self
    }
}
