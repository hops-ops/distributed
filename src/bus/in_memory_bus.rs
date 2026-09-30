//! In-memory bus — the dev/test reference implementation of [`Bus`] +
//! [`BusConsumer`].
//!
//! `send`/`listen` use named queues with competing-consumer (point-to-point)
//! semantics: a message is popped by exactly one consumer. `publish`/`subscribe`
//! use named **retained logs** with a per-subscriber cursor, so every subscriber
//! sees every event (fan-out) — the same log+offset shape the Postgres fan-out
//! transport uses, in memory.
//!
//! It is intentionally simple (no durability, no redelivery on nack) — for tests
//! and local development. Use a real transport for production reliability.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use super::source::{MessageSource, ReceivedMessage};
use super::wake::Notify;
use super::{run_source, Bus, BusConsumer, MessageRouter, RunOptions, TransportError};
use super::{Message, OrderedDelivery};
use crate::projection_protocol::{ProjectionEpoch, ProjectionSource};

type Queues = Arc<Mutex<HashMap<String, VecDeque<Message>>>>;
type Topics = Arc<Mutex<HashMap<String, Vec<Message>>>>;
type TopicCursors = Arc<Mutex<HashMap<String, usize>>>;

fn lock_poisoned(what: &str) -> TransportError {
    TransportError::permanent(format!("in-memory bus {what} lock poisoned"))
}

/// In-memory [`Bus`] + [`BusConsumer`] for tests and local development.
///
/// Cheap to clone (shares the same queues/logs), so competing listeners and
/// fan-out subscribers can each hold a clone.
#[derive(Clone)]
pub struct InMemoryBus {
    queues: Queues,
    topics: Topics,
    source_epoch: ProjectionEpoch,
    wake: Arc<Notify>,
}

impl Default for InMemoryBus {
    fn default() -> Self {
        Self {
            queues: Queues::default(),
            topics: Topics::default(),
            source_epoch: ProjectionEpoch::new(format!("instance-{}", uuid::Uuid::now_v7()))
                .expect("an in-memory bus UUID is a valid projection source epoch"),
            wake: Arc::new(Notify::new()),
        }
    }
}

impl InMemoryBus {
    pub fn new() -> Self {
        Self::default()
    }

    fn enqueue(&self, message: Message) -> Result<(), TransportError> {
        self.queues
            .lock()
            .map_err(|_| lock_poisoned("queue"))?
            .entry(message.name().to_string())
            .or_default()
            .push_back(message);
        self.wake.notify_waiters();
        Ok(())
    }

    fn append(&self, message: Message) -> Result<(), TransportError> {
        let mut topics = self.topics.lock().map_err(|_| lock_poisoned("topic"))?;
        if let Some(id) = message.id() {
            if let Some(existing) = topics
                .values()
                .flat_map(|log| log.iter())
                .find(|existing| existing.id() == Some(id))
            {
                validate_topic_retry(existing, &message)?;
                return Ok(());
            }
        }
        topics
            .entry(message.name().to_string())
            .or_default()
            .push(message);
        self.wake.notify_waiters();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn ordered_topic_evidence(&self, name: &str, position: u64) -> OrderedDelivery {
        OrderedDelivery::new(
            ProjectionSource::new("in_memory.topic", name.as_bytes().to_vec())
                .expect("an in-memory topic name is a valid projection source partition"),
            self.source_epoch.clone(),
            position,
            true,
        )
        .expect("an in-memory topic position is valid ordered-delivery evidence")
    }
}

/// Verify that an ambiguous publish retry using an existing stable ID is the
/// exact causal envelope already retained in the ordered topic log.
///
/// Trace metadata can legitimately change when the producer retries after an
/// unknown acknowledgement, so it is excluded. Causation is included because
/// it is part of the projector's canonical input identity.
fn validate_topic_retry(existing: &Message, retry: &Message) -> Result<(), TransportError> {
    let matches = existing.name == retry.name
        && existing.kind == retry.kind
        && existing.payload == retry.payload
        && existing.content_type == retry.content_type
        && existing.causation_id() == retry.causation_id();
    if matches {
        return Ok(());
    }

    Err(TransportError::permanent(format!(
        "in-memory bus ordered-topic message ID {:?} was reused with a different \
         name, kind, payload, content type, or causation ID",
        retry.id()
    )))
}

impl Bus for InMemoryBus {
    async fn send_message(&self, message: Message) -> Result<(), TransportError> {
        self.enqueue(message)
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        self.append(message)
    }
}

impl BusConsumer for InMemoryBus {
    async fn listen<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        let names = router.subscription_plan().commands;
        let source = QueueSource {
            queues: self.queues.clone(),
            names,
            wake: Arc::clone(&self.wake),
        };
        run_source(router, source, options).await
    }

    async fn subscribe<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        let names = router.subscription_plan().events;
        let source = TopicSource {
            topics: self.topics.clone(),
            names,
            cursors: TopicCursors::default(),
            source_epoch: self.source_epoch.clone(),
            wake: Arc::clone(&self.wake),
        };
        run_source(router, source, options).await
    }
}

/// Competing-consumer source over the named queues: each message is popped once.
struct QueueSource {
    queues: Queues,
    names: Vec<String>,
    wake: Arc<Notify>,
}

impl QueueSource {
    fn has_available(&self) -> Result<bool, TransportError> {
        let queues = self.queues.lock().map_err(|_| lock_poisoned("queue"))?;
        Ok(self
            .names
            .iter()
            .any(|name| queues.get(name).is_some_and(|queue| !queue.is_empty())))
    }
}

impl MessageSource for QueueSource {
    type Received = InMemoryReceived;

    fn transport_name(&self) -> &'static str {
        "in_memory"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        let mut queues = self.queues.lock().map_err(|_| lock_poisoned("queue"))?;
        for name in &self.names {
            if let Some(message) = queues.get_mut(name).and_then(VecDeque::pop_front) {
                return Ok(Some(InMemoryReceived {
                    message,
                    ordered: None,
                    topic_settlement: None,
                }));
            }
        }
        Ok(None)
    }

    async fn wait(&mut self) -> Result<(), TransportError> {
        let notified = self.wake.notified();
        if self.has_available()? {
            return Ok(());
        }
        notified.await;
        Ok(())
    }
}

/// Fan-out source over the named retained logs: each `TopicSource` has its own
/// cursor, so every subscriber reads every event.
struct TopicSource {
    topics: Topics,
    names: Vec<String>,
    cursors: TopicCursors,
    source_epoch: ProjectionEpoch,
    wake: Arc<Notify>,
}

impl TopicSource {
    fn has_available(&self) -> Result<bool, TransportError> {
        let topics = self.topics.lock().map_err(|_| lock_poisoned("topic"))?;
        let cursors = self
            .cursors
            .lock()
            .map_err(|_| lock_poisoned("topic cursor"))?;
        Ok(self.names.iter().any(|name| {
            topics
                .get(name)
                .is_some_and(|log| cursors.get(name).copied().unwrap_or_default() < log.len())
        }))
    }
}

impl MessageSource for TopicSource {
    type Received = InMemoryReceived;

    fn transport_name(&self) -> &'static str {
        "in_memory"
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        let topics = self.topics.lock().map_err(|_| lock_poisoned("topic"))?;
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| lock_poisoned("topic cursor"))?;
        for name in &self.names {
            let Some(log) = topics.get(name) else {
                continue;
            };
            let cursor = cursors.entry(name.clone()).or_insert(0);
            if *cursor < log.len() {
                let message = log[*cursor].clone();
                let position = u64::try_from(*cursor).map_err(|_| {
                    TransportError::permanent(
                        "in-memory topic position cannot fit the projection cursor domain",
                    )
                })?;
                let source = ProjectionSource::new("in_memory.topic", name.as_bytes().to_vec())
                    .map_err(|error| TransportError::permanent(error.to_string()))?;
                let ordered =
                    OrderedDelivery::new(source, self.source_epoch.clone(), position, true)
                        .map_err(|error| TransportError::permanent(error.to_string()))?;
                return Ok(Some(InMemoryReceived {
                    message,
                    ordered: Some(ordered),
                    topic_settlement: Some(TopicSettlement {
                        cursors: Arc::clone(&self.cursors),
                        name: name.clone(),
                        position: *cursor,
                    }),
                }));
            }
        }
        Ok(None)
    }

    async fn wait(&mut self) -> Result<(), TransportError> {
        let notified = self.wake.notified();
        if self.has_available()? {
            return Ok(());
        }
        notified.await;
        Ok(())
    }
}

struct TopicSettlement {
    cursors: TopicCursors,
    name: String,
    position: usize,
}

impl TopicSettlement {
    fn ack(self) -> Result<(), TransportError> {
        let mut cursors = self
            .cursors
            .lock()
            .map_err(|_| lock_poisoned("topic cursor"))?;
        let cursor = cursors.entry(self.name).or_insert(0);
        match (*cursor).cmp(&self.position) {
            std::cmp::Ordering::Equal => {
                *cursor = cursor.checked_add(1).ok_or_else(|| {
                    TransportError::permanent("in-memory topic cursor overflowed")
                })?;
                Ok(())
            }
            std::cmp::Ordering::Greater => Ok(()),
            std::cmp::Ordering::Less => Err(TransportError::permanent(
                "in-memory topic delivery was acknowledged out of order",
            )),
        }
    }
}

/// In-memory delivery. Queue settlement remains a no-op because queue messages
/// are popped on receive. Retained-topic cursors advance only on `ack`; `nack`
/// leaves the exact gap-free position available for redelivery.
pub struct InMemoryReceived {
    message: Message,
    ordered: Option<OrderedDelivery>,
    topic_settlement: Option<TopicSettlement>,
}

impl ReceivedMessage for InMemoryReceived {
    fn message(&self) -> &Message {
        &self.message
    }
    fn ordered_delivery(&self) -> Option<&OrderedDelivery> {
        self.ordered.as_ref()
    }
    async fn ack(self) -> Result<(), TransportError> {
        match self.topic_settlement {
            Some(settlement) => settlement.ack(),
            None => Ok(()),
        }
    }
    async fn nack(self, _reason: &str) -> Result<(), TransportError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{Handlers, MessageKind};
    use crate::trace_context::{CAUSATION_ID, TRACEPARENT};
    use std::future::Future;
    use std::time::Duration;

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

    fn recorder() -> Arc<Mutex<Vec<String>>> {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn command_service(rec: Arc<Mutex<Vec<String>>>) -> Arc<Handlers> {
        Arc::new(Handlers::new().on_command("work", move |msg: &Message| {
            let rec = rec.clone();
            let name = msg.name().to_string();
            async move {
                rec.lock().unwrap().push(name);
                Ok(())
            }
        }))
    }

    fn event_service(rec: Arc<Mutex<Vec<String>>>) -> Arc<Handlers> {
        Arc::new(Handlers::new().on_event("evt", move |msg: &Message| {
            let rec = rec.clone();
            let id = msg.id().unwrap_or("?").to_string();
            async move {
                rec.lock().unwrap().push(id);
                Ok(())
            }
        }))
    }

    #[test]
    fn send_then_listen_dispatches_each_command() {
        let bus = InMemoryBus::new();
        for _ in 0..3 {
            block_on(bus.send("work", b"{}".to_vec())).unwrap();
        }
        let rec = recorder();
        block_on(bus.listen(command_service(rec.clone()), RunOptions::idempotent())).unwrap();
        assert_eq!(
            rec.lock().unwrap().len(),
            3,
            "the listener handles all 3 commands"
        );
    }

    #[test]
    fn listen_is_point_to_point_each_message_popped_once() {
        // Two competing sources over the same queue: each message goes to one.
        let bus = InMemoryBus::new();
        for i in 0..4 {
            block_on(bus.send_message(
                Message::new("work", MessageKind::Command, b"{}".to_vec()).with_id(format!("m{i}")),
            ))
            .unwrap();
        }
        let mut a = QueueSource {
            queues: bus.queues.clone(),
            names: vec!["work".to_string()],
            wake: Arc::clone(&bus.wake),
        };
        let mut b = QueueSource {
            queues: bus.queues.clone(),
            names: vec!["work".to_string()],
            wake: Arc::clone(&bus.wake),
        };
        let mut got = Vec::new();
        // Alternate; each pop removes the message (competing).
        for _ in 0..4 {
            if let Some(r) = block_on(a.recv()).unwrap() {
                got.push(r.message().id().unwrap().to_string());
            }
            if let Some(r) = block_on(b.recv()).unwrap() {
                got.push(r.message().id().unwrap().to_string());
            }
        }
        got.sort();
        assert_eq!(
            got,
            vec!["m0", "m1", "m2", "m3"],
            "each message delivered exactly once"
        );
        // Queue now drained for both.
        assert!(block_on(a.recv()).unwrap().is_none());
        assert!(block_on(b.recv()).unwrap().is_none());
    }

    #[test]
    fn publish_then_subscribe_fans_out_to_every_subscriber() {
        let bus = InMemoryBus::new();
        for i in 0..3 {
            block_on(bus.publish_message(
                Message::new("evt", MessageKind::Event, b"{}".to_vec()).with_id(format!("e{i}")),
            ))
            .unwrap();
        }
        // Two independent subscribers; each gets every event (own cursor).
        let a = recorder();
        let b = recorder();
        block_on(bus.subscribe(event_service(a.clone()), RunOptions::idempotent())).unwrap();
        block_on(bus.subscribe(event_service(b.clone()), RunOptions::idempotent())).unwrap();
        let mut a_ids = a.lock().unwrap().clone();
        let mut b_ids = b.lock().unwrap().clone();
        a_ids.sort();
        b_ids.sort();
        assert_eq!(a_ids, vec!["e0", "e1", "e2"]);
        assert_eq!(b_ids, vec!["e0", "e1", "e2"]);
    }

    #[tokio::test]
    async fn one_publish_wakes_every_waiting_topic_subscriber() {
        let bus = InMemoryBus::new();
        let first = recorder();
        let second = recorder();
        let first_task = {
            let bus = bus.clone();
            let service = event_service(first.clone());
            tokio::spawn(async move {
                bus.subscribe(service, RunOptions::idempotent().wait_when_idle())
                    .await
            })
        };
        let second_task = {
            let bus = bus.clone();
            let service = event_service(second.clone());
            tokio::spawn(async move {
                bus.subscribe(service, RunOptions::idempotent().wait_when_idle())
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while bus.wake.waiter_count() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both subscribers registered their idle waiters");

        bus.publish_message(
            Message::new("evt", MessageKind::Event, b"{}".to_vec()).with_id("broadcast"),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while first.lock().unwrap().is_empty() || second.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both waiting subscribers handled the publish");

        assert_eq!(first.lock().unwrap().as_slice(), ["broadcast"]);
        assert_eq!(second.lock().unwrap().as_slice(), ["broadcast"]);
        first_task.abort();
        second_task.abort();
    }

    #[test]
    fn topic_nack_redelivers_exact_gap_free_position_and_ack_advances() {
        let bus = InMemoryBus::new();
        for id in ["e0", "e1"] {
            block_on(bus.publish_message(
                Message::new("evt", MessageKind::Event, b"{}".to_vec()).with_id(id),
            ))
            .unwrap();
        }
        let mut source = TopicSource {
            topics: bus.topics.clone(),
            names: vec!["evt".into()],
            cursors: TopicCursors::default(),
            source_epoch: bus.source_epoch.clone(),
            wake: Arc::clone(&bus.wake),
        };

        let first = block_on(source.recv()).unwrap().unwrap();
        assert_eq!(first.message().id(), Some("e0"));
        assert_eq!(first.ordered_delivery().unwrap().position(), 0);
        assert!(first.ordered_delivery().unwrap().is_gap_free());
        block_on(first.nack("transient")).unwrap();

        let replay = block_on(source.recv()).unwrap().unwrap();
        assert_eq!(replay.message().id(), Some("e0"));
        assert_eq!(replay.ordered_delivery().unwrap().position(), 0);
        block_on(replay.ack()).unwrap();

        let second = block_on(source.recv()).unwrap().unwrap();
        assert_eq!(second.message().id(), Some("e1"));
        assert_eq!(second.ordered_delivery().unwrap().position(), 1);
        block_on(second.ack()).unwrap();
        assert!(block_on(source.recv()).unwrap().is_none());
    }

    #[test]
    fn topic_stable_id_retry_reuses_original_position_and_ignores_trace_only_metadata() {
        let bus = InMemoryBus::new();
        let original = Message::new("evt", MessageKind::Event, br#"{"value":1}"#.to_vec())
            .with_id("e0")
            .with_metadata(CAUSATION_ID, "command-1")
            .with_metadata(TRACEPARENT, "first-span");
        let retry = Message::new("evt", MessageKind::Event, br#"{"value":1}"#.to_vec())
            .with_id("e0")
            .with_metadata(CAUSATION_ID, "command-1")
            .with_metadata(TRACEPARENT, "retry-span");
        block_on(bus.publish_message(original)).unwrap();
        block_on(bus.publish_message(retry)).unwrap();

        let topics = bus.topics.lock().unwrap();
        let log = topics.get("evt").unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].traceparent(), Some("first-span"));
        drop(topics);

        let mut source = TopicSource {
            topics: bus.topics.clone(),
            names: vec!["evt".into()],
            cursors: TopicCursors::default(),
            source_epoch: bus.source_epoch.clone(),
            wake: Arc::clone(&bus.wake),
        };
        let received = block_on(source.recv()).unwrap().unwrap();
        assert_eq!(received.message().id(), Some("e0"));
        assert_eq!(received.ordered_delivery().unwrap().position(), 0);
        block_on(received.ack()).unwrap();
        assert!(block_on(source.recv()).unwrap().is_none());
    }

    #[test]
    fn topic_stable_id_reuse_with_different_causal_envelope_is_permanent() {
        let bus = InMemoryBus::new();
        let original = Message::new("evt", MessageKind::Event, br#"{"value":1}"#.to_vec())
            .with_id("e0")
            .with_metadata(CAUSATION_ID, "command-1");
        block_on(bus.publish_message(original)).unwrap();

        let cases = [
            Message::new("other", MessageKind::Event, br#"{"value":1}"#.to_vec())
                .with_id("e0")
                .with_metadata(CAUSATION_ID, "command-1"),
            Message::new("evt", MessageKind::Command, br#"{"value":1}"#.to_vec())
                .with_id("e0")
                .with_metadata(CAUSATION_ID, "command-1"),
            Message::new("evt", MessageKind::Event, br#"{"value":2}"#.to_vec())
                .with_id("e0")
                .with_metadata(CAUSATION_ID, "command-1"),
            Message::new("evt", MessageKind::Event, br#"{"value":1}"#.to_vec())
                .with_id("e0")
                .with_metadata(CAUSATION_ID, "command-2"),
        ];
        for retry in cases {
            let error = block_on(bus.publish_message(retry)).unwrap_err();
            assert!(error.is_permanent());
            assert!(error.message().contains("message ID"));
        }

        let topics = bus.topics.lock().unwrap();
        assert_eq!(topics.values().map(Vec::len).sum::<usize>(), 1);
    }

    #[test]
    fn unknown_command_is_acked_and_ignored() {
        // A command with no handler is ignored by the runner (acked), not an error.
        let bus = InMemoryBus::new();
        block_on(bus.send("unrelated", b"{}".to_vec())).unwrap();
        block_on(bus.send("work", b"{}".to_vec())).unwrap();
        let rec = recorder();
        block_on(bus.listen(command_service(rec.clone()), RunOptions::idempotent())).unwrap();
        assert_eq!(rec.lock().unwrap().clone(), vec!["work"]);
    }

    #[test]
    fn handler_error_does_not_panic_the_loop() {
        let bus = InMemoryBus::new();
        block_on(bus.send("work", b"{}".to_vec())).unwrap();
        let handlers: Arc<Handlers> = Arc::new(
            Handlers::new().on_command("work", |_: &Message| async move {
                Err(TransportError::permanent("no"))
            }),
        );
        // Default failure policy dead-letters the permanent failure; in-memory
        // dead_letter is a no-op nack, so the run completes cleanly.
        block_on(bus.listen(handlers, RunOptions::idempotent())).unwrap();
    }
}
