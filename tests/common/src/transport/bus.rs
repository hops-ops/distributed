//! The bus contract as one suite that every `Bus + BusConsumer` runs.
//!
//! The contract is written down in `docs/bus-contract.md`. A transport joins
//! the suite by implementing [`BusHarness`] and calling
//! `bus_conformance_tests!(TheHarness);` in its test target.
//!
//! Where transports legitimately differ, the harness declares it in
//! [`BusHarness::CAPABILITIES`] and the scenario checks the declared
//! behaviour, whichever way it goes. Nothing is left out silently.
//!
//! Every consumer names its group through its router
//! ([`Handlers::named`]), never through the bus, so one scenario reads the
//! same on every transport. Event scenarios first drain an empty subscription
//! for each group: the group then exists on every transport, so events
//! published afterwards reach it. (RabbitMQ drops events no queue is bound
//! for.)
//!
//! Nothing here waits on a timer to make a test pass. A consumer that runs
//! until shutdown is shut down by its own handler once it has seen what the
//! scenario expects; the one timeout ([`GUARD`]) only turns a hang into a
//! failure.

use std::collections::BTreeMap;
use std::future::{ready, Future, Ready};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    Bus, BusConsumer, DeadLetter, Handlers, RunOptions, Shutdown, TransportError, DEAD_LETTER_EVENT,
};
use distributed::bus::{Message, MessageKind};
use distributed::outbox_worker::{SOURCE_AGGREGATE_ID, SOURCE_AGGREGATE_TYPE};

/// How a test target connects its transport.
pub trait BusHarness: Sized {
    type Bus: Bus + BusConsumer;

    /// Where this transport differs from the others.
    const CAPABILITIES: BusCapabilities;

    /// Set up an isolated transport (fresh database, namespace or stream).
    /// `None` when the service this transport needs is not configured.
    fn start() -> impl Future<Output = Option<Self>>;

    /// A connected bus with no consumer group of its own. Every bus of one
    /// harness shares the same queues and logs.
    fn bus(&self) -> impl Future<Output = Self::Bus>;
}

/// The declared differences between transports.
#[derive(Clone, Copy, Debug)]
pub struct BusCapabilities {
    /// How `send` / `listen` behaves.
    pub commands: KindCapabilities,
    /// How `publish` / `subscribe` behaves.
    pub events: KindCapabilities,
    /// While consumers of one group join or leave, a message one of them has
    /// handled but not yet acknowledged may be delivered to another (Kafka's
    /// partition rebalance). Without it, competing consumers handle each
    /// message exactly once.
    pub rebalance_may_redeliver: bool,
    /// Metadata keys the transport adds to every delivery (Knative's
    /// CloudEvent `source`). Everything else a consumer receives is what
    /// was sent.
    pub added_metadata: &'static [&'static str],
}

/// The declared behaviour of one message kind.
#[derive(Clone, Copy, Debug)]
pub struct KindCapabilities {
    /// A stable message id produced twice is stored once. Otherwise both
    /// copies are delivered, identical, and the consumer's inbox removes the
    /// second.
    pub dedupes_ids: bool,
    /// A message retried at once is delivered again before the messages
    /// behind it. Otherwise a later message may overtake it.
    pub retry_holds_position: bool,
    /// Messages of different names are delivered in the order they were
    /// produced. Otherwise only each name keeps its own order.
    pub ordered_across_names: bool,
}

impl KindCapabilities {
    /// One ordered log or queue for the kind that stores a stable id once.
    pub const LOG: Self = Self {
        dedupes_ids: true,
        retry_holds_position: true,
        ordered_across_names: true,
    };
}

impl BusCapabilities {
    fn of(&self, kind: MessageKind) -> KindCapabilities {
        match kind {
            MessageKind::Command => self.commands,
            MessageKind::Event => self.events,
        }
    }
}

/// Turns a hung consumer into a failed test. Never what a test waits for.
const GUARD: Duration = Duration::from_secs(120);

const COMMAND: &str = "conformance.ship";
const EVENT: &str = "conformance.shipped";
/// A second name of each kind, for the cross-name scenarios.
const OTHER_COMMAND: &str = "conformance.pack";
const OTHER_EVENT: &str = "conformance.packed";
const GROUP: &str = "conformance";
const OTHER_GROUP: &str = "conformance-audit";
const DEAD_LETTER_GROUP: &str = "conformance-dead-letters";

fn name_for(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Command => COMMAND,
        MessageKind::Event => EVENT,
    }
}

fn other_name_for(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Command => OTHER_COMMAND,
        MessageKind::Event => OTHER_EVENT,
    }
}

fn message(kind: MessageKind, id: &str) -> Message {
    Message::new(name_for(kind), kind, b"{}".to_vec()).with_id(id)
}

async fn produce<B: Bus>(bus: &B, message: Message) {
    match message.kind {
        MessageKind::Command => bus.send_message(message).await.expect("send command"),
        MessageKind::Event => bus.publish_message(message).await.expect("publish event"),
    }
}

async fn produce_all<B: Bus>(bus: &B, kind: MessageKind, ids: &[String]) {
    for id in ids {
        produce(bus, message(kind, id)).await;
    }
}

fn numbered(prefix: &str, total: usize) -> Vec<String> {
    (0..total).map(|n| format!("{prefix}{n}")).collect()
}

/// The ids a consumer handled, in the order it handled them.
#[derive(Clone, Default)]
struct Handled(Arc<Mutex<Vec<String>>>);

impl Handled {
    fn push(&self, message: &Message) -> usize {
        let mut handled = self.0.lock().unwrap();
        handled.push(message.id().unwrap_or_default().to_string());
        handled.len()
    }

    fn ids(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }

    fn sorted(&self) -> Vec<String> {
        let mut ids = self.ids();
        ids.sort();
        ids
    }
}

/// A router for `group` that runs `handle` for every name in `names`.
fn handlers<F>(kind: MessageKind, group: &str, names: &[&str], handle: F) -> Arc<Handlers>
where
    F: Fn(&Message) -> Result<(), TransportError> + Clone + Send + Sync + 'static,
{
    let mut handlers = Handlers::new().named(group);
    for name in names {
        let handle = handle.clone();
        let handler = move |message: &Message| -> Ready<Result<(), TransportError>> {
            ready(handle(message))
        };
        handlers = match kind {
            MessageKind::Command => handlers.on_command(*name, handler),
            MessageKind::Event => handlers.on_event(*name, handler),
        };
    }
    Arc::new(handlers)
}

/// A router for `group` that records every message of `names` it handles.
fn recording(kind: MessageKind, group: &str, names: &[&str], handled: &Handled) -> Arc<Handlers> {
    let handled = handled.clone();
    handlers(kind, group, names, move |message| {
        handled.push(message);
        Ok(())
    })
}

async fn consume<B: BusConsumer>(
    bus: &B,
    kind: MessageKind,
    router: Arc<Handlers>,
    options: RunOptions,
) -> Result<(), TransportError> {
    let run = async {
        match kind {
            MessageKind::Command => bus.listen(router, options).await,
            MessageKind::Event => bus.subscribe(router, options).await,
        }
    };
    tokio::time::timeout(GUARD, run)
        .await
        .expect("the consumer returned")
}

/// Run `router` until it is idle.
async fn drain<B: BusConsumer>(bus: &B, kind: MessageKind, router: Arc<Handlers>) {
    consume(bus, kind, router, RunOptions::until_idle())
        .await
        .expect("the consumer drains");
}

/// Make `group` exist for `names` before anything is published to them.
async fn establish<B: BusConsumer>(bus: &B, kind: MessageKind, group: &str, names: &[&str]) {
    if kind == MessageKind::Command {
        return;
    }
    drain(bus, kind, handlers(kind, group, names, |_| Ok(()))).await;
}

/// Id, payload, metadata and content type arrive as they were sent.
pub async fn message_round_trips_unchanged<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus().await;
    establish(&bus, kind, GROUP, &[name_for(kind)]).await;

    let mut sent = Message::new(name_for(kind), kind, br#"{"n":1}"#.to_vec())
        .with_id("m1")
        .with_metadata("x-tenant", "acme");
    sent.content_type = "application/vnd.conformance+json".to_string();
    produce(&bus, sent).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = seen.clone();
    drain(
        &bus,
        kind,
        handlers(kind, GROUP, &[name_for(kind)], move |message| {
            store.lock().unwrap().push(message.clone());
            Ok(())
        }),
    )
    .await;

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "delivered once");
    let received = &seen[0];
    assert_eq!(received.name(), name_for(kind));
    assert_eq!(received.kind, kind);
    assert_eq!(received.id(), Some("m1"));
    assert_eq!(received.payload(), br#"{"n":1}"#);
    assert_eq!(received.content_type, "application/vnd.conformance+json");
    let added = H::CAPABILITIES.added_metadata;
    for key in added {
        assert!(received.metadata(key).is_some(), "{key} is added");
    }
    let sent: Vec<_> = received
        .metadata
        .iter()
        .filter(|(key, _)| !added.contains(&key.as_str()))
        .cloned()
        .collect();
    assert_eq!(
        sent,
        vec![("x-tenant".to_string(), "acme".to_string())],
        "user metadata survives and no wire header leaks into it"
    );
}

/// Metadata naming a reserved wire header is refused at the publish
/// boundary, in any letter case, before anything reaches the transport.
pub async fn reserved_header_override_is_refused<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus().await;
    for key in [
        "x-distributed-id",
        "X-Distributed-Kind",
        "x-distributed-content-type",
        "X-DISTRIBUTED-NAME",
    ] {
        let forged = message(kind, "forged").with_metadata(key, "command");
        let error = match kind {
            MessageKind::Command => bus.send_message(forged).await,
            MessageKind::Event => bus.publish_message(forged).await,
        }
        .expect_err("a reserved metadata key is refused");
        assert!(error.is_permanent(), "{key}: {error}");
    }
}

/// Consumers of one group share the work: run side by side, they handle each
/// message once between them, and acknowledge it, so the group's next run
/// finds nothing. For commands this is point-to-point delivery; for events,
/// competition inside a group.
pub async fn one_group_shares_the_work<H: BusHarness>(harness: &H, kind: MessageKind) {
    let producer = harness.bus().await;
    establish(&producer, kind, GROUP, &[name_for(kind)]).await;
    let expected = numbered("w", 8);
    produce_all(&producer, kind, &expected).await;

    let handled = Handled::default();
    let shutdown = Shutdown::new();
    let replica = || {
        let (handled, shutdown, total) = (handled.clone(), shutdown.clone(), expected.len());
        handlers(kind, GROUP, &[name_for(kind)], move |message| {
            if handled.push(message) >= total {
                shutdown.trigger();
            }
            Ok(())
        })
    };
    let (a, b) = (harness.bus().await, harness.bus().await);
    let (ra, rb) = tokio::join!(
        consume(
            &a,
            kind,
            replica(),
            RunOptions::until_shutdown(shutdown.clone())
        ),
        consume(
            &b,
            kind,
            replica(),
            RunOptions::until_shutdown(shutdown.clone())
        ),
    );
    ra.expect("replica a stops cleanly");
    rb.expect("replica b stops cleanly");

    let mut distinct = handled.sorted();
    distinct.dedup();
    assert_eq!(distinct, expected, "every message was handled");
    if !H::CAPABILITIES.rebalance_may_redeliver {
        assert_eq!(handled.sorted(), expected, "and handled once");
    }

    let again = Handled::default();
    drain(
        &harness.bus().await,
        kind,
        recording(kind, GROUP, &[name_for(kind)], &again),
    )
    .await;
    assert_eq!(again.ids(), Vec::<String>::new(), "all were acknowledged");
}

/// Every group sees every event.
pub async fn every_group_sees_every_event<H: BusHarness>(harness: &H) {
    let kind = MessageKind::Event;
    let producer = harness.bus().await;
    for group in [GROUP, OTHER_GROUP] {
        establish(&producer, kind, group, &[EVENT]).await;
    }
    let expected = numbered("e", 4);
    produce_all(&producer, kind, &expected).await;

    for group in [GROUP, OTHER_GROUP] {
        let handled = Handled::default();
        drain(
            &harness.bus().await,
            kind,
            recording(kind, group, &[EVENT], &handled),
        )
        .await;
        assert_eq!(handled.ids(), expected, "group {group} sees every event");
    }
}

/// The group named by the consumer keeps its position between runs: a
/// second run of the group starts after the events the first acknowledged.
pub async fn a_named_group_keeps_its_position<H: BusHarness>(harness: &H) {
    let kind = MessageKind::Event;
    let bus = harness.bus().await;
    establish(&bus, kind, GROUP, &[EVENT]).await;
    produce_all(&bus, kind, &numbered("first", 2)).await;

    let first = Handled::default();
    drain(&bus, kind, recording(kind, GROUP, &[EVENT], &first)).await;
    assert_eq!(first.ids(), numbered("first", 2));

    produce_all(&bus, kind, &numbered("second", 2)).await;
    let second = Handled::default();
    drain(
        &harness.bus().await,
        kind,
        recording(kind, GROUP, &[EVENT], &second),
    )
    .await;
    assert_eq!(second.ids(), numbered("second", 2));
}

/// Fails `id` the first time it is handled, with a retryable error.
fn fail_first(
    id: &'static str,
    handled: &Handled,
) -> impl Fn(&Message) -> Result<(), TransportError> + Clone + Send + Sync + 'static {
    let attempts = Arc::new(Mutex::new(BTreeMap::<String, u32>::new()));
    let handled = handled.clone();
    move |message| {
        let this = message.id().unwrap_or_default().to_string();
        let attempt = {
            let mut attempts = attempts.lock().unwrap();
            let attempt = attempts.entry(this.clone()).or_default();
            *attempt += 1;
            *attempt
        };
        if this == id && attempt == 1 {
            return Err(TransportError::retryable("not yet"));
        }
        handled.push(message);
        Ok(())
    }
}

/// A retryable failure is delivered again, once, and the messages behind it
/// are still delivered.
pub async fn retryable_failure_is_redelivered<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus().await;
    establish(&bus, kind, GROUP, &[name_for(kind)]).await;
    produce(&bus, message(kind, "flaky")).await;
    produce(&bus, message(kind, "next")).await;

    let handled = Handled::default();
    drain(
        &bus,
        kind,
        handlers(
            kind,
            GROUP,
            &[name_for(kind)],
            fail_first("flaky", &handled),
        ),
    )
    .await;

    assert_eq!(
        handled.sorted(),
        vec!["flaky".to_string(), "next".to_string()]
    );
}

/// Reordered delivery: whether a message retried at once can be overtaken
/// by the one behind it is declared, and holds.
pub async fn retried_message_keeps_its_declared_position<H: BusHarness>(
    harness: &H,
    kind: MessageKind,
) {
    let bus = harness.bus().await;
    establish(&bus, kind, GROUP, &[name_for(kind)]).await;
    let sent = numbered("r", 3);
    produce_all(&bus, kind, &sent).await;

    let handled = Handled::default();
    drain(
        &bus,
        kind,
        handlers(kind, GROUP, &[name_for(kind)], fail_first("r0", &handled)),
    )
    .await;

    if H::CAPABILITIES.of(kind).retry_holds_position {
        assert_eq!(handled.ids(), sent, "the retried message kept its place");
    } else {
        assert_eq!(handled.sorted(), sent, "each delivered once, in any order");
    }
}

/// A permanent failure is stored on the bus as a dead letter, is not
/// delivered again, and does not hold up the messages behind it.
pub async fn permanent_failure_is_dead_lettered<H: BusHarness>(harness: &H, kind: MessageKind) {
    let poison = message(kind, "poison");
    let reason = dead_letter_scenario(harness, kind, poison, |message| {
        if message.id() == Some("poison") {
            Err(TransportError::permanent("unprocessable"))
        } else {
            Ok(())
        }
    })
    .await;
    assert!(reason.contains("unprocessable"), "{reason}");
}

/// A payload the handler cannot decode is a permanent failure: dead-lettered
/// once, and the messages behind it are handled.
pub async fn undecodable_payload_is_dead_lettered<H: BusHarness>(harness: &H, kind: MessageKind) {
    let garbage = Message::new(name_for(kind), kind, vec![0xff, 0xfe, b'{']).with_id("poison");
    let reason = dead_letter_scenario(harness, kind, garbage, |message| {
        serde_json::from_slice::<serde_json::Value>(message.payload())
            .map(|_| ())
            .map_err(|error| TransportError::permanent(format!("undecodable: {error}")))
    })
    .await;
    assert!(reason.contains("undecodable"), "{reason}");
}

/// Produce `poison` then a good message, run `handle` twice, and check the
/// poison message was dead-lettered once and the good one handled once.
/// Returns the dead letter's reason.
async fn dead_letter_scenario<H, F>(
    harness: &H,
    kind: MessageKind,
    poison: Message,
    handle: F,
) -> String
where
    H: BusHarness,
    F: Fn(&Message) -> Result<(), TransportError> + Clone + Send + Sync + 'static,
{
    let bus = harness.bus().await;
    let dead_letters = harness.bus().await;
    establish(&bus, kind, GROUP, &[name_for(kind)]).await;
    establish(
        &dead_letters,
        MessageKind::Event,
        DEAD_LETTER_GROUP,
        &[DEAD_LETTER_EVENT],
    )
    .await;
    produce(&bus, poison).await;
    produce(&bus, message(kind, "ok")).await;

    let handled = Handled::default();
    let router = || {
        let (handled, handle) = (handled.clone(), handle.clone());
        handlers(kind, GROUP, &[name_for(kind)], move |message| {
            handle(message)?;
            handled.push(message);
            Ok(())
        })
    };
    drain(&bus, kind, router()).await;
    // A second run finds nothing: the poison message was acknowledged.
    drain(&bus, kind, router()).await;
    assert_eq!(handled.ids(), vec!["ok".to_string()]);

    let stored = Arc::new(Mutex::new(Vec::new()));
    let store = stored.clone();
    drain(
        &dead_letters,
        MessageKind::Event,
        handlers(
            MessageKind::Event,
            DEAD_LETTER_GROUP,
            &[DEAD_LETTER_EVENT],
            move |message| {
                store.lock().unwrap().push(message.clone());
                Ok(())
            },
        ),
    )
    .await;
    let stored = stored.lock().unwrap();
    assert_eq!(stored.len(), 1, "one dead letter was stored");
    let dead: DeadLetter =
        serde_json::from_slice(stored[0].payload()).expect("dead letter payload decodes");
    assert_eq!(dead.message.id(), Some("poison"));
    assert_eq!(dead.message.name(), name_for(kind));
    assert_eq!(dead.attempts, 1);
    assert_eq!(dead.consumer, GROUP, "the consumer is named");
    dead.reason
}

/// One stream's messages of different names reach a consumer of both names
/// in the order they were produced (`packed` before `shipped`), when the
/// transport declares it; each name keeps its own order regardless.
pub async fn one_stream_keeps_its_order_across_names<H: BusHarness>(
    harness: &H,
    kind: MessageKind,
) {
    let names = [other_name_for(kind), name_for(kind)];
    let bus = harness.bus().await;
    establish(&bus, kind, GROUP, &names).await;
    let mut sent = Vec::new();
    for n in 0..3 {
        for name in names {
            let id = format!("{name}-{n}");
            let message = Message::new(name, kind, b"{}".to_vec())
                .with_id(&id)
                .with_metadata(SOURCE_AGGREGATE_TYPE, "conformance")
                .with_metadata(SOURCE_AGGREGATE_ID, "stream-1");
            produce(&bus, message).await;
            sent.push(id);
        }
    }

    let handled = Handled::default();
    drain(&bus, kind, recording(kind, GROUP, &names, &handled)).await;

    let handled = handled.ids();
    if H::CAPABILITIES.of(kind).ordered_across_names {
        assert_eq!(handled, sent, "produced order across names");
    } else {
        for name in names {
            let of = |ids: &[String]| {
                ids.iter()
                    .filter(|id| id.starts_with(&format!("{name}-")))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            assert_eq!(of(&handled), of(&sent), "{name} keeps its own order");
        }
    }
}

/// Duplicate delivery: the same message produced twice is stored once where
/// the transport dedupes stable ids; otherwise both copies arrive, identical,
/// so a consumer's inbox can drop the second.
pub async fn a_duplicate_is_stored_once_or_delivered_identically<H: BusHarness>(
    harness: &H,
    kind: MessageKind,
) {
    let bus = harness.bus().await;
    establish(&bus, kind, GROUP, &[name_for(kind)]).await;
    let original = Message::new(name_for(kind), kind, br#"{"n":7}"#.to_vec()).with_id("dup");
    produce(&bus, original.clone()).await;
    produce(&bus, original.clone()).await;
    produce(&bus, message(kind, "after")).await;

    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = seen.clone();
    drain(
        &bus,
        kind,
        handlers(kind, GROUP, &[name_for(kind)], move |message| {
            store.lock().unwrap().push(message.clone());
            Ok(())
        }),
    )
    .await;

    let seen = seen.lock().unwrap();
    let copies: Vec<&Message> = seen.iter().filter(|m| m.id() == Some("dup")).collect();
    let expected = if H::CAPABILITIES.of(kind).dedupes_ids {
        1
    } else {
        2
    };
    assert_eq!(copies.len(), expected, "copies of the duplicate delivered");
    for copy in copies {
        assert_eq!(copy.payload(), original.payload());
        assert_eq!(copy.name(), original.name());
    }
    assert_eq!(
        seen.iter().filter(|m| m.id() == Some("after")).count(),
        1,
        "the message behind the duplicate is delivered once"
    );
}

/// A running consumer stays up while the transport is quiet, handles a
/// message produced later, and returns `Ok` when shut down.
pub async fn quiet_consumer_runs_until_shutdown<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus().await;
    let producer = harness.bus().await;
    establish(&bus, kind, GROUP, &[name_for(kind)]).await;
    produce(&producer, message(kind, "early")).await;

    let handled = Handled::default();
    let shutdown = Shutdown::new();
    let (early_tx, early_rx) = tokio::sync::oneshot::channel::<()>();
    let early_tx = Arc::new(Mutex::new(Some(early_tx)));
    let router = {
        let (handled, shutdown) = (handled.clone(), shutdown.clone());
        handlers(kind, GROUP, &[name_for(kind)], move |message| {
            handled.push(message);
            match message.id() {
                Some("early") => {
                    if let Some(tx) = early_tx.lock().unwrap().take() {
                        let _ = tx.send(());
                    }
                }
                _ => shutdown.trigger(),
            }
            Ok(())
        })
    };
    let run = consume(
        &bus,
        kind,
        router,
        RunOptions::until_shutdown(shutdown.clone()),
    );
    let drive = async {
        // The consumer has handled everything there was; produce the next.
        early_rx
            .await
            .expect("the consumer handled the early message");
        produce(&producer, message(kind, "late")).await;
    };
    let (result, ()) = tokio::join!(run, drive);
    result.expect("shutdown ends the run cleanly");
    assert_eq!(handled.ids(), vec!["early".to_string(), "late".to_string()]);
}

/// Instantiate the bus contract for one [`BusHarness`].
#[macro_export]
macro_rules! bus_conformance_tests {
    ($harness:ty) => {
        mod bus_conformance {
            use super::*;
            use distributed::bus::MessageKind;
            use $crate::transport::bus::{self as suite, BusHarness};

            macro_rules! scenario {
                ($name:ident, $scenario:ident, $kind:ident) => {
                    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                    async fn $name() {
                        let Some(harness) = <$harness as BusHarness>::start().await else {
                            return;
                        };
                        suite::$scenario(&harness, MessageKind::$kind).await;
                    }
                };
                ($name:ident, $scenario:ident) => {
                    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                    async fn $name() {
                        let Some(harness) = <$harness as BusHarness>::start().await else {
                            return;
                        };
                        suite::$scenario(&harness).await;
                    }
                };
            }

            scenario!(
                command_round_trips_unchanged,
                message_round_trips_unchanged,
                Command
            );
            scenario!(
                event_round_trips_unchanged,
                message_round_trips_unchanged,
                Event
            );
            scenario!(
                command_reserved_header_is_refused,
                reserved_header_override_is_refused,
                Command
            );
            scenario!(
                event_reserved_header_is_refused,
                reserved_header_override_is_refused,
                Event
            );
            scenario!(
                commands_are_point_to_point_in_a_group,
                one_group_shares_the_work,
                Command
            );
            scenario!(
                events_compete_inside_a_group,
                one_group_shares_the_work,
                Event
            );
            scenario!(events_fan_out_to_every_group, every_group_sees_every_event);
            scenario!(
                named_group_keeps_its_position,
                a_named_group_keeps_its_position
            );
            scenario!(
                retryable_command_is_redelivered,
                retryable_failure_is_redelivered,
                Command
            );
            scenario!(
                retryable_event_is_redelivered,
                retryable_failure_is_redelivered,
                Event
            );
            scenario!(
                retried_command_keeps_declared_position,
                retried_message_keeps_its_declared_position,
                Command
            );
            scenario!(
                retried_event_keeps_declared_position,
                retried_message_keeps_its_declared_position,
                Event
            );
            scenario!(
                permanent_command_failure_is_dead_lettered,
                permanent_failure_is_dead_lettered,
                Command
            );
            scenario!(
                permanent_event_failure_is_dead_lettered,
                permanent_failure_is_dead_lettered,
                Event
            );
            scenario!(
                undecodable_command_is_dead_lettered,
                undecodable_payload_is_dead_lettered,
                Command
            );
            scenario!(
                undecodable_event_is_dead_lettered,
                undecodable_payload_is_dead_lettered,
                Event
            );
            scenario!(
                commands_keep_order_across_names,
                one_stream_keeps_its_order_across_names,
                Command
            );
            scenario!(
                events_keep_order_across_names,
                one_stream_keeps_its_order_across_names,
                Event
            );
            scenario!(
                duplicate_command_is_deduped_or_identical,
                a_duplicate_is_stored_once_or_delivered_identically,
                Command
            );
            scenario!(
                duplicate_event_is_deduped_or_identical,
                a_duplicate_is_stored_once_or_delivered_identically,
                Event
            );
            scenario!(
                quiet_command_consumer_runs_until_shutdown,
                quiet_consumer_runs_until_shutdown,
                Command
            );
            scenario!(
                quiet_event_consumer_runs_until_shutdown,
                quiet_consumer_runs_until_shutdown,
                Event
            );
        }
    };
}
