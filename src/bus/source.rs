//! Direct-transport receive traits.
//!
//! A direct broker client (Postgres, RabbitMQ, Kafka, NATS, or the in-memory
//! dev/test adapter) pulls messages with [`MessageSource`] and settles each
//! one through [`ReceivedMessage`]. The [`run_source`](super::run_source) runner
//! drives that loop: it dispatches through `Service::dispatch_message` and only
//! then asks the adapter to acknowledge.
//!
//! These are the *direct* receive shape. Knative / HTTP CloudEvents is a
//! separate ingress shape (the platform invokes an endpoint; there is no local
//! poll loop) and is intentionally not modeled through this trait.

use std::future::Future;

use super::{Message, OrderedDelivery, TransportError};

/// A transport a runner can pull messages from, one at a time.
///
/// `recv` resolves to:
/// - `Ok(Some(received))` — a message to dispatch and then settle;
/// - `Ok(None)` — the source is drained/closed; the runner stops **gracefully**
///   (this is the shutdown signal — an adapter wires its own stop into `recv`);
/// - `Err(e)` — a transport-level receive failure, surfaced by the runner rather
///   than swallowed.
///
/// The future is `Send` so the runner can be driven on multi-threaded executors.
pub trait MessageSource: Send {
    /// The settle handle for a received message.
    type Received: ReceivedMessage;

    /// Stable label used for framework metrics.
    fn transport_name(&self) -> &'static str {
        "unknown"
    }

    /// Receive the next message, if any.
    fn recv(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Received>, TransportError>> + Send + '_;

    /// Park until `recv` might return work. Used only when
    /// [`IdlePolicy::Wait`](super::IdlePolicy::Wait) is set.
    ///
    /// Sources used with this policy must override the default with a real
    /// notification, broker long-poll, or bounded timer. Failing explicitly is
    /// safer than letting an unsupported source spin at executor speed.
    fn wait(&mut self) -> impl Future<Output = Result<(), TransportError>> + Send + '_ {
        async {
            Err(TransportError::permanent(
                "message source does not support idle waiting",
            ))
        }
    }
}

/// A message received from a transport, plus the means to settle it.
///
/// Settlement consumes the value so a message can be settled exactly once. `ack`
/// and `nack` are the universal primitives every transport supports; an adapter
/// maps them to its native operation (a row completion, a delivery ack, an
/// offset commit, a stream ack, …). `dead_letter` and `park` default to `nack`
/// so a message is never silently dropped; adapters with native dead-letter or
/// parking support should override them.
pub trait ReceivedMessage: Send {
    /// The canonical message to dispatch.
    fn message(&self) -> &Message;

    /// Adapter-authenticated ordered source position for causal projection.
    ///
    /// The default is deliberately unqualified. A source must override this
    /// only when it has a durable numeric source position and an explicit epoch;
    /// delivery tags, attempts, timestamps, UUIDs, and arrival order do not
    /// qualify. The runner forwards this sealed value separately from message
    /// metadata.
    fn ordered_delivery(&self) -> Option<&OrderedDelivery> {
        None
    }

    /// A permanent decode failure for this delivery, if the transport could not
    /// reconstruct the message from its stored representation.
    ///
    /// Defaults to `None`: most adapters either decode successfully or fail the
    /// whole `recv`. An adapter that can claim a row/offset *before* decoding it
    /// (so the claim must be settled even when decoding fails) returns the
    /// classified error here. The runner treats `Some(err)` as a permanent
    /// failure routed through the [`FailurePolicy`](super::FailurePolicy) — the
    /// same path as a permanent dispatch failure — so a corrupt row is
    /// dead-lettered/parked rather than ack-and-ignored as an empty message.
    fn decode_error(&self) -> Option<&TransportError> {
        None
    }

    /// Acknowledge successful handling. The transport removes the message.
    ///
    /// The runner calls this only after consumer execution has succeeded (and,
    /// in inbox mode, after the inbox receipt has committed).
    fn ack(self) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Negatively acknowledge so the transport redelivers the message later.
    fn nack(self, reason: &str) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Route the message to a dead-letter destination.
    ///
    /// Defaults to [`nack`](ReceivedMessage::nack): an adapter without a native
    /// dead-letter destination keeps the message redeliverable rather than
    /// dropping it, so a `DeadLetter` policy degrades to redelivery until the
    /// adapter implements real dead-lettering.
    fn dead_letter(self, reason: &str) -> impl Future<Output = Result<(), TransportError>> + Send
    where
        Self: Sized,
    {
        self.nack(reason)
    }

    /// Hold the message for manual intervention without acking or redelivering.
    ///
    /// Defaults to [`nack`](ReceivedMessage::nack) for adapters without native
    /// parking; such adapters keep the message redeliverable rather than
    /// dropping it.
    fn park(self, reason: &str) -> impl Future<Output = Result<(), TransportError>> + Send
    where
        Self: Sized,
    {
        self.nack(reason)
    }
}
