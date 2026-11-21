//! Where a consumer puts a message it has given up on.
//!
//! Dead-lettering is done by the runner, above the transport: it hands the
//! message and the reason to a [`DeadLetterSink`], and only when the sink has
//! stored it does it acknowledge the original delivery. No adapter implements
//! its own dead-letter operation.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use super::{Bus, Message, MessageKind, TransportError};

/// Name of the event a [`PublishDeadLetters`] sink publishes.
pub const DEAD_LETTER_EVENT: &str = "distributed.dead_letter";

/// A message a consumer could not handle, and why.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DeadLetter {
    /// The message exactly as it was delivered.
    pub message: Message,
    /// The failure that ended handling.
    pub reason: String,
    /// The consumer group that gave up, or an empty string if it has none.
    pub consumer: String,
    /// How many times the message had been delivered to this consumer.
    pub attempts: u32,
}

/// Durable storage for dead letters.
pub trait DeadLetterSink: Send + Sync {
    /// Store `dead`. The runner acknowledges the original delivery only after
    /// this returns `Ok`; on `Err` the delivery is retained and the run ends.
    fn dead_letter<'a>(
        &'a self,
        dead: DeadLetter,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>>;
}

/// Publishes each dead letter as a [`DEAD_LETTER_EVENT`] event.
///
/// The event payload is the JSON [`DeadLetter`]. Its id is derived from the
/// consumer and the original message id, so a broker that deduplicates by id
/// stores one dead letter when a delivery is dead-lettered twice.
pub struct PublishDeadLetters<B>(pub B);

impl<B: Bus> DeadLetterSink for PublishDeadLetters<B> {
    fn dead_letter<'a>(
        &'a self,
        dead: DeadLetter,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async move {
            let payload = serde_json::to_vec(&dead).map_err(|error| {
                TransportError::permanent(format!("encode dead letter: {error}"))
            })?;
            let mut event = Message::new(DEAD_LETTER_EVENT, MessageKind::Event, payload);
            if let Some(id) = dead.message.id() {
                event = event.with_id(format!("dead-letter:{}:{id}", dead.consumer));
            }
            self.0.publish_message(event).await
        })
    }
}

/// Keeps dead letters in memory. For tests and the in-memory bus.
#[derive(Clone, Default)]
pub struct InMemoryDeadLetters(Arc<Mutex<Vec<DeadLetter>>>);

impl InMemoryDeadLetters {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every dead letter stored so far, oldest first.
    pub fn all(&self) -> Vec<DeadLetter> {
        match self.0.lock() {
            Ok(stored) => stored.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl DeadLetterSink for InMemoryDeadLetters {
    fn dead_letter<'a>(
        &'a self,
        dead: DeadLetter,
    ) -> Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'a>> {
        Box::pin(async move {
            self.0
                .lock()
                .map_err(|_| TransportError::permanent("in-memory dead-letter lock poisoned"))?
                .push(dead);
            Ok(())
        })
    }
}
