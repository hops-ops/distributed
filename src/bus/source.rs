//! What a transport implements so a consumer can read from it.
//!
//! A transport does three things: hand over the next delivery, acknowledge
//! it, or ask for it again after a delay. Everything else — how often to
//! retry, when to give up, where a given-up message goes, when to stop — is
//! decided by [`run_source`](super::run_source) from
//! [`RunOptions`](super::RunOptions).
//!
//! Knative / HTTP CloudEvents is a push shape (the platform calls an
//! endpoint) and is not modeled here.

use std::future::Future;
use std::time::Duration;

use super::{Message, OrderedDelivery, TransportError};

/// A transport a runner pulls deliveries from, one at a time.
pub trait MessageSource: Send {
    /// A delivery and the means to settle it.
    type Received: ReceivedMessage;

    /// Stable label used for framework metrics.
    fn transport_name(&self) -> &'static str {
        "unknown"
    }

    /// Whether each delivery can be settled independently of the others, in
    /// any order, while later deliveries are still being received.
    ///
    /// True only for per-message acknowledgements (for example NATS JetStream
    /// explicit acks). Positional (offset commit) and lease-based sources keep
    /// the default `false`, and the runner then processes one delivery at a
    /// time even when the router has several delivery lanes.
    fn settles_independently(&self) -> bool {
        false
    }

    /// Take the next delivery if one is available.
    ///
    /// `Ok(None)` means nothing is available now; it does not mean the source
    /// is closed. A source may wait briefly for a delivery (a broker fetch
    /// with a timeout) before returning `None`, but must return.
    ///
    /// The runner may drop this future when it is told to shut down. A
    /// delivery claimed by a dropped call must become available again by the
    /// transport's own redelivery (a lease expiring, an unacked message).
    fn recv(
        &mut self,
    ) -> impl Future<Output = Result<Option<Self::Received>, TransportError>> + Send + '_;

    /// Resolve when [`recv`](Self::recv) is worth calling again after it
    /// returned `None`.
    ///
    /// The runner calls this only when it runs until shutdown, so that an
    /// empty source costs nothing. A source whose `recv` already waits for the
    /// broker keeps the default, which resolves at once. A source whose `recv`
    /// returns immediately must wait here (a poll interval or a notification).
    fn ready(&mut self) -> impl Future<Output = ()> + Send + '_ {
        std::future::ready(())
    }
}

/// One delivery of a message.
///
/// Settling consumes the value, so a delivery is settled once.
pub trait ReceivedMessage: Send {
    /// The message to dispatch.
    fn message(&self) -> &Message;

    /// How many times this message has been delivered to this consumer,
    /// counting this delivery. The first delivery is 1.
    ///
    /// A source that cannot count deliveries keeps the default of 1; the
    /// runner's [`max_attempts`](super::RunOptions::max_attempts) and backoff
    /// growth then have no effect for it.
    fn attempts(&self) -> u32 {
        1
    }

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

    /// Why this delivery could not be decoded into a message, if it could not.
    ///
    /// A source that claims a delivery before decoding it returns the error
    /// here so the claim can still be settled. The runner treats it as a
    /// permanent failure and never dispatches the message.
    fn decode_error(&self) -> Option<&TransportError> {
        None
    }

    /// Handling is finished; never deliver this message to this consumer
    /// again.
    fn ack(self) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// Deliver this message again, no sooner than `delay` from now.
    ///
    /// A positional source (an offset or a log cursor) cannot deliver a later
    /// message first; it holds its position for `delay`.
    fn nack(self, delay: Duration) -> impl Future<Output = Result<(), TransportError>> + Send;
}
