//! The shared async publish boundary.
//!
//! Producing is more uniform than consuming, so outbox dispatch and any other
//! producer uses a single [`MessagePublisher`]. Each adapter documents its
//! durable *publish threshold* — the point `publish` may resolve `Ok`:
//!
//! - Postgres: the outbox-backed bus row committed, or a committed insert into a
//!   separate queue table;
//! - RabbitMQ: publisher confirm;
//! - Kafka: the producer send acknowledged per the configured `acks`;
//! - NATS JetStream: a JetStream publish ack;
//! - Knative / HTTP: a successful response from the Broker/sink;
//! - in-memory: accepted into the in-memory queue/log.
//!
//! Until that threshold, `publish` must not resolve `Ok`. An unknown outcome
//! must surface as `Err` so the outbox row stays retryable — duplicate delivery
//! is acceptable under at-least-once, silent loss is not.

use std::future::Future;

use super::{Message, TransportError};

/// Publishes canonical [`Message`]s to a transport.
///
/// `publish` resolves `Ok` only once the adapter's durable publish threshold is
/// reached; any failure or unknown outcome is `Err`. The error's
/// [retryability](TransportError) lets the caller (the outbox dispatcher) decide
/// whether to keep the row retryable.
pub trait MessagePublisher: Send + Sync {
    /// Publish a single message.
    fn publish(
        &self,
        message: Message,
    ) -> impl Future<Output = Result<(), TransportError>> + Send + '_;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::MessageKind;
    use std::future::Future;
    use std::sync::Mutex;

    fn block_on<F: Future>(future: F) -> F::Output {
        use std::ptr;
        use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        let waker = unsafe { Waker::from_raw(RawWaker::new(ptr::null(), &VTABLE)) };
        let mut cx = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
                return output;
            }
        }
    }

    /// Publisher that records publishes and fails once it reaches `fail_at`.
    struct CountingPublisher {
        published: Mutex<Vec<String>>,
        fail_at: Option<usize>,
    }

    impl MessagePublisher for CountingPublisher {
        async fn publish(&self, message: Message) -> Result<(), TransportError> {
            let mut published = self.published.lock().unwrap();
            if self.fail_at == Some(published.len()) {
                return Err(TransportError::retryable("publish failed"));
            }
            published.push(message.name().to_string());
            Ok(())
        }
    }

    fn msg(name: &str) -> Message {
        Message::new(name, MessageKind::Event, b"{}".to_vec())
    }

    #[test]
    fn publish_reports_the_adapter_error() {
        let publisher = CountingPublisher {
            published: Mutex::new(Vec::new()),
            fail_at: Some(1),
        };
        block_on(publisher.publish(msg("a"))).unwrap();
        assert!(block_on(publisher.publish(msg("b"))).is_err());
        assert_eq!(*publisher.published.lock().unwrap(), vec!["a"]);
    }
}
