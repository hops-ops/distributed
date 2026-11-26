//! In-memory bus — the dev/test reference implementation of [`Bus`] +
//! [`BusConsumer`].
//!
//! Commands go to one queue and events to one log, both in publish order
//! across every message name, the same shape the SQL buses have.
//!
//! - `send`/`listen` is point-to-point: a listener takes the oldest command it
//!   handles out of the queue, so each command reaches one listener. A nacked
//!   command goes back to the head of the queue and is delivered again before
//!   the commands behind it.
//! - `publish`/`subscribe` fans out: each consumer group has its own position
//!   in the log, so every group sees every event, in publish order. One
//!   subscriber of a group reads at a time; the others wait until it stops. A
//!   nacked event is delivered again from the same position. A stable message
//!   id is stored once.
//!
//! It keeps nothing across restarts and does not honour a nack's delay.

use std::collections::{HashMap, VecDeque};
use std::future::poll_fn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Poll, Waker};
use std::time::Duration;

use super::source::{MessageSource, ReceivedMessage};
use super::{
    run_source, Bus, BusConsumer, MessageRouter, PublishDeadLetters, RunOptions, TransportError,
};
use super::{Message, OrderedDelivery, WireEnvelope};
use crate::projection_protocol::{ProjectionEpoch, ProjectionSource};

/// Counts everything that could make an empty source non-empty, and wakes the
/// consumers waiting for it.
#[derive(Default)]
struct Arrivals {
    count: AtomicU64,
    waiting: Mutex<Vec<Waker>>,
}

impl Arrivals {
    fn seen(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }

    fn notify(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        let waiting = match self.waiting.lock() {
            Ok(mut waiting) => std::mem::take(&mut *waiting),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        for waker in waiting {
            waker.wake();
        }
    }

    /// Resolves once something has arrived after `seen` was read.
    async fn after(&self, seen: u64) {
        poll_fn(|cx| {
            if self.seen() != seen {
                return Poll::Ready(());
            }
            let mut waiting = match self.waiting.lock() {
                Ok(waiting) => waiting,
                Err(poisoned) => poisoned.into_inner(),
            };
            // `notify` counts before it takes the wakers, so an arrival
            // between the check above and this lock is seen here.
            if self.seen() != seen {
                return Poll::Ready(());
            }
            waiting.push(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

fn lock_poisoned() -> TransportError {
    TransportError::permanent("in-memory bus lock poisoned")
}

/// One event in the log, with its position among the events of its name.
struct Entry {
    message: Message,
    name_position: u64,
}

/// A consumer group's place in the log.
#[derive(Default)]
struct Group {
    /// Index of the first log entry the group has not acknowledged.
    cursor: usize,
    /// The subscriber reading for the group, if one is.
    holder: Option<u64>,
    /// The entry most recently handed back, and how often.
    handed_back: Option<(usize, u32)>,
}

#[derive(Default)]
struct State {
    /// Each queued command with the number of times it has been delivered.
    queue: VecDeque<(Message, u32)>,
    log: Vec<Entry>,
    /// Log index of every stable message id.
    ids: HashMap<String, usize>,
    /// Events appended so far per name.
    name_counts: HashMap<String, u64>,
    groups: HashMap<String, Group>,
    next_subscriber: u64,
}

type Shared = Arc<Mutex<State>>;

fn lock(state: &Shared) -> Result<MutexGuard<'_, State>, TransportError> {
    state.lock().map_err(|_| lock_poisoned())
}

/// In-memory [`Bus`] + [`BusConsumer`] for tests and local development.
///
/// Cheap to clone (shares the same queue and log), so competing listeners and
/// fan-out subscribers can each hold a clone.
#[derive(Clone)]
pub struct InMemoryBus {
    state: Shared,
    arrivals: Arc<Arrivals>,
    source_epoch: ProjectionEpoch,
}

impl Default for InMemoryBus {
    fn default() -> Self {
        Self {
            state: Shared::default(),
            arrivals: Arc::default(),
            source_epoch: ProjectionEpoch::new(format!("instance-{}", uuid::Uuid::now_v7()))
                .expect("an in-memory bus UUID is a valid projection source epoch"),
        }
    }
}

impl InMemoryBus {
    pub fn new() -> Self {
        Self::default()
    }

    /// Dead letters are published to this bus unless the run names a sink.
    fn with_dead_letters(&self, options: RunOptions) -> RunOptions {
        options.or_dead_letters(|| Arc::new(PublishDeadLetters(self.clone())))
    }

    fn enqueue(&self, message: Message) -> Result<(), TransportError> {
        lock(&self.state)?.queue.push_back((message, 0));
        self.arrivals.notify();
        Ok(())
    }

    fn append(&self, message: Message) -> Result<(), TransportError> {
        let mut state = lock(&self.state)?;
        if let Some(index) = message.id().and_then(|id| state.ids.get(id)) {
            return validate_topic_retry(&state.log[*index].message, &message);
        }
        let count = state.name_counts.entry(message.name.clone()).or_insert(0);
        let name_position = *count;
        *count += 1;
        if let Some(id) = message.id() {
            let index = state.log.len();
            state.ids.insert(id.to_string(), index);
        }
        state.log.push(Entry {
            message,
            name_position,
        });
        drop(state);
        self.arrivals.notify();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn published_ids(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("test state lock")
            .log
            .iter()
            .filter_map(|entry| entry.message.id().map(str::to_owned))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn ordered_topic_evidence(&self, name: &str, position: u64) -> OrderedDelivery {
        ordered_evidence(&self.source_epoch, name, position)
            .expect("an in-memory topic position is valid ordered-delivery evidence")
    }
}

/// Ordered-delivery evidence for the `position`th event named `name`. Every
/// subscriber of a name sees each of its events in order, so the evidence is
/// gap-free within the name.
fn ordered_evidence(
    epoch: &ProjectionEpoch,
    name: &str,
    position: u64,
) -> Result<OrderedDelivery, TransportError> {
    let source = ProjectionSource::new("in_memory.topic", name.as_bytes().to_vec())
        .map_err(|error| TransportError::permanent(error.to_string()))?;
    OrderedDelivery::new(source, epoch.clone(), position, true)
        .map_err(|error| TransportError::permanent(error.to_string()))
}

/// Verify that an ambiguous publish retry using an existing stable ID is the
/// exact causal envelope already retained in the ordered log.
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
        WireEnvelope::validate(&message)?;
        self.enqueue(message)
    }

    async fn publish_message(&self, message: Message) -> Result<(), TransportError> {
        WireEnvelope::validate(&message)?;
        self.append(message)
    }
}

impl BusConsumer for InMemoryBus {
    async fn listen<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        let source = QueueSource {
            state: self.state.clone(),
            names: router.subscription_plan().commands,
            arrivals: Arc::clone(&self.arrivals),
            seen: 0,
        };
        run_source(router, source, self.with_dead_letters(options)).await
    }

    async fn subscribe<R: MessageRouter>(
        &self,
        router: Arc<R>,
        options: RunOptions,
    ) -> Result<(), TransportError> {
        let source =
            self.topic_source(router.subscription_plan().events, router.consumer_group())?;
        run_source(router, source, self.with_dead_letters(options)).await
    }
}

impl InMemoryBus {
    /// A named group keeps its position between runs, as a durable consumer
    /// does. An unnamed subscriber is a group of its own and reads every
    /// retained event.
    fn topic_source(
        &self,
        names: Vec<String>,
        group: Option<&str>,
    ) -> Result<TopicSource, TransportError> {
        let mut state = lock(&self.state)?;
        let id = state.next_subscriber;
        state.next_subscriber += 1;
        let (group, private) = match group {
            Some(group) => (group.to_string(), false),
            // Not a valid group name, so it cannot collide with one.
            None => (format!("\u{0}subscriber-{id}"), true),
        };
        Ok(TopicSource {
            state: self.state.clone(),
            names,
            group,
            private,
            id,
            source_epoch: self.source_epoch.clone(),
            arrivals: Arc::clone(&self.arrivals),
            seen: 0,
        })
    }
}

/// Competing-consumer source over the command queue: each command is taken
/// once.
struct QueueSource {
    state: Shared,
    names: Vec<String>,
    arrivals: Arc<Arrivals>,
    seen: u64,
}

impl MessageSource for QueueSource {
    type Received = InMemoryReceived;

    fn transport_name(&self) -> &'static str {
        "in_memory"
    }

    async fn ready(&mut self) {
        self.arrivals.after(self.seen).await
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        self.seen = self.arrivals.seen();
        let mut state = lock(&self.state)?;
        let Some(index) = state
            .queue
            .iter()
            .position(|(message, _)| self.names.iter().any(|name| *name == message.name))
        else {
            return Ok(None);
        };
        let (message, delivered) = state.queue.remove(index).expect("index was just found");
        Ok(Some(InMemoryReceived {
            message,
            attempts: delivered.saturating_add(1),
            ordered: None,
            settlement: Settlement::Queue {
                state: Arc::clone(&self.state),
                arrivals: Arc::clone(&self.arrivals),
            },
        }))
    }
}

/// Fan-out source over the log for one consumer group.
struct TopicSource {
    state: Shared,
    names: Vec<String>,
    group: String,
    /// The group belongs to this subscriber alone and ends with it.
    private: bool,
    id: u64,
    source_epoch: ProjectionEpoch,
    arrivals: Arc<Arrivals>,
    seen: u64,
}

impl Drop for TopicSource {
    fn drop(&mut self) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if self.private {
            state.groups.remove(&self.group);
        } else if let Some(group) = state.groups.get_mut(&self.group) {
            if group.holder == Some(self.id) {
                group.holder = None;
            }
        }
        drop(state);
        // A subscriber of the group waiting for its turn can read now.
        self.arrivals.notify();
    }
}

impl MessageSource for TopicSource {
    type Received = InMemoryReceived;

    fn transport_name(&self) -> &'static str {
        "in_memory"
    }

    async fn ready(&mut self) {
        self.arrivals.after(self.seen).await
    }

    async fn recv(&mut self) -> Result<Option<Self::Received>, TransportError> {
        self.seen = self.arrivals.seen();
        let mut state = lock(&self.state)?;
        let State { log, groups, .. } = &mut *state;
        let group = groups.entry(self.group.clone()).or_default();
        match group.holder {
            Some(holder) if holder != self.id => return Ok(None),
            _ => group.holder = Some(self.id),
        }
        let Some((index, entry)) = log
            .iter()
            .enumerate()
            .skip(group.cursor)
            .find(|(_, entry)| self.names.iter().any(|name| *name == entry.message.name))
        else {
            return Ok(None);
        };
        let handed_back = match group.handed_back {
            Some((at, times)) if at == index => times,
            _ => 0,
        };
        Ok(Some(InMemoryReceived {
            message: entry.message.clone(),
            attempts: handed_back.saturating_add(1),
            ordered: Some(ordered_evidence(
                &self.source_epoch,
                &entry.message.name,
                entry.name_position,
            )?),
            settlement: Settlement::Topic {
                state: Arc::clone(&self.state),
                arrivals: Arc::clone(&self.arrivals),
                group: self.group.clone(),
                index,
            },
        }))
    }
}

/// How a delivery is settled: a command was taken out of the queue on
/// receive; an event is still in the log behind its group's cursor.
enum Settlement {
    Queue {
        state: Shared,
        arrivals: Arc<Arrivals>,
    },
    Topic {
        state: Shared,
        arrivals: Arc<Arrivals>,
        group: String,
        index: usize,
    },
}

/// In-memory delivery. `nack` puts a command back at the head of the queue; a
/// group's cursor moves only on `ack`, so a nacked event is read again from
/// the same position. The delay is not honoured.
pub struct InMemoryReceived {
    message: Message,
    attempts: u32,
    ordered: Option<OrderedDelivery>,
    settlement: Settlement,
}

impl ReceivedMessage for InMemoryReceived {
    fn message(&self) -> &Message {
        &self.message
    }
    fn attempts(&self) -> u32 {
        self.attempts
    }
    fn ordered_delivery(&self) -> Option<&OrderedDelivery> {
        self.ordered.as_ref()
    }
    async fn ack(self) -> Result<(), TransportError> {
        let Settlement::Topic {
            state,
            group,
            index,
            ..
        } = self.settlement
        else {
            return Ok(());
        };
        let mut state = lock(&state)?;
        let group = state.groups.entry(group).or_default();
        // A cursor already past the entry means it was acknowledged before.
        group.cursor = group.cursor.max(index + 1);
        if matches!(group.handed_back, Some((at, _)) if at == index) {
            group.handed_back = None;
        }
        Ok(())
    }
    async fn nack(self, _delay: Duration) -> Result<(), TransportError> {
        match self.settlement {
            Settlement::Topic {
                state,
                arrivals,
                group,
                index,
            } => {
                lock(&state)?.groups.entry(group).or_default().handed_back =
                    Some((index, self.attempts));
                arrivals.notify();
            }
            Settlement::Queue { state, arrivals } => {
                lock(&state)?
                    .queue
                    .push_front((self.message, self.attempts));
                arrivals.notify();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::{Handlers, MessageKind};
    use crate::trace_context::{CAUSATION_ID, TRACEPARENT};
    use std::future::Future;

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
        block_on(bus.listen(command_service(rec.clone()), RunOptions::until_idle())).unwrap();
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
            state: bus.state.clone(),
            names: vec!["work".to_string()],
            arrivals: Arc::clone(&bus.arrivals),
            seen: 0,
        };
        let mut b = QueueSource {
            state: bus.state.clone(),
            names: vec!["work".to_string()],
            arrivals: Arc::clone(&bus.arrivals),
            seen: 0,
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
        block_on(bus.subscribe(event_service(a.clone()), RunOptions::until_idle())).unwrap();
        block_on(bus.subscribe(event_service(b.clone()), RunOptions::until_idle())).unwrap();
        let mut a_ids = a.lock().unwrap().clone();
        let mut b_ids = b.lock().unwrap().clone();
        a_ids.sort();
        b_ids.sort();
        assert_eq!(a_ids, vec!["e0", "e1", "e2"]);
        assert_eq!(b_ids, vec!["e0", "e1", "e2"]);
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
        let mut source = bus.topic_source(vec!["evt".into()], None).unwrap();

        let first = block_on(source.recv()).unwrap().unwrap();
        assert_eq!(first.message().id(), Some("e0"));
        assert_eq!(first.ordered_delivery().unwrap().position(), 0);
        assert!(first.ordered_delivery().unwrap().is_gap_free());
        block_on(first.nack(Duration::ZERO)).unwrap();

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

        let state = bus.state.lock().unwrap();
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.log[0].message.traceparent(), Some("first-span"));
        drop(state);

        let mut source = bus.topic_source(vec!["evt".into()], None).unwrap();
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

        assert_eq!(bus.state.lock().unwrap().log.len(), 1);
    }

    #[test]
    fn unknown_command_is_acked_and_ignored() {
        // A command with no handler is ignored by the runner (acked), not an error.
        let bus = InMemoryBus::new();
        block_on(bus.send("unrelated", b"{}".to_vec())).unwrap();
        block_on(bus.send("work", b"{}".to_vec())).unwrap();
        let rec = recorder();
        block_on(bus.listen(command_service(rec.clone()), RunOptions::until_idle())).unwrap();
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
        block_on(bus.listen(handlers, RunOptions::until_idle())).unwrap();
    }
}
