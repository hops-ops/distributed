//! The bus contract as one suite that every `Bus + BusConsumer` runs.
//!
//! The contract is written down in `docs/bus-contract.md`. A transport joins
//! the suite by implementing [`BusHarness`] and calling
//! `bus_conformance_tests!(TheHarness);` in its test target.
//!
//! Event scenarios first drain an empty subscription for each group. A group
//! exists on every transport from then on, so events published afterwards are
//! delivered to it. (RabbitMQ drops events that no queue is bound for.)

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use distributed::bus::{
    Bus, BusConsumer, DeadLetter, Handlers, RunOptions, Shutdown, TransportError, DEAD_LETTER_EVENT,
};
use distributed::microsvc::{Message, MessageKind};

/// How a test target connects its transport.
pub trait BusHarness: Sized {
    type Bus: Bus + BusConsumer;

    /// Set up an isolated transport (fresh database, namespace or stream).
    /// `None` when the service this transport needs is not configured.
    fn start() -> impl Future<Output = Option<Self>>;

    /// A connected bus for the consumer group `group`. Every bus of one
    /// harness shares the same queues and logs.
    fn bus(&self, group: &str) -> impl Future<Output = Self::Bus>;
}

const COMMAND: &str = "conformance.ship";
const EVENT: &str = "conformance.shipped";
const GROUP: &str = "conformance";
const DEAD_LETTER_GROUP: &str = "conformance-dead-letters";

fn name_for(kind: MessageKind) -> &'static str {
    match kind {
        MessageKind::Command => COMMAND,
        MessageKind::Event => EVENT,
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

async fn consume<B: BusConsumer>(
    bus: &B,
    kind: MessageKind,
    handlers: Handlers,
    options: RunOptions,
) -> Result<(), TransportError> {
    let handlers = Arc::new(handlers);
    match kind {
        MessageKind::Command => bus.listen(handlers, options).await,
        MessageKind::Event => bus.subscribe(handlers, options).await,
    }
}

fn on(kind: MessageKind, name: &str, handlers: Handlers, seen: Seen) -> Handlers {
    let handler = move |message: &Message| {
        seen.lock().unwrap().push(message.clone());
        async { Ok(()) }
    };
    match kind {
        MessageKind::Command => handlers.on_command(name, handler),
        MessageKind::Event => handlers.on_event(name, handler),
    }
}

type Seen = Arc<Mutex<Vec<Message>>>;

fn ids(seen: &Seen) -> Vec<String> {
    seen.lock()
        .unwrap()
        .iter()
        .map(|message| message.id().unwrap_or_default().to_string())
        .collect()
}

/// Make `group` exist for `name` before anything is published to it.
async fn establish<B: BusConsumer>(bus: &B, kind: MessageKind, name: &str) {
    if kind == MessageKind::Command {
        return;
    }
    let handlers = Handlers::new().on_event(name, |_: &Message| async { Ok(()) });
    bus.subscribe(Arc::new(handlers), RunOptions::until_idle())
        .await
        .expect("an empty subscription drains");
}

/// Id, payload, metadata and content type arrive as they were sent.
pub async fn message_round_trips_unchanged<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus(GROUP).await;
    establish(&bus, kind, name_for(kind)).await;

    let mut sent = Message::new(name_for(kind), kind, br#"{"n":1}"#.to_vec())
        .with_id("m1")
        .with_metadata("x-tenant", "acme");
    sent.content_type = "application/vnd.conformance+json".to_string();
    produce(&bus, sent).await;

    let seen = Seen::default();
    consume(
        &bus,
        kind,
        on(kind, name_for(kind), Handlers::new(), seen.clone()),
        RunOptions::until_idle(),
    )
    .await
    .expect("consumer drains");

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "delivered once");
    let received = &seen[0];
    assert_eq!(received.name(), name_for(kind));
    assert_eq!(received.kind, kind);
    assert_eq!(received.id(), Some("m1"));
    assert_eq!(received.payload(), br#"{"n":1}"#);
    assert_eq!(received.content_type, "application/vnd.conformance+json");
    assert!(
        received
            .metadata
            .iter()
            .any(|(key, value)| key.eq_ignore_ascii_case("x-tenant") && value == "acme"),
        "user metadata survives: {:?}",
        received.metadata
    );
}

/// A retryable failure is delivered again, and the messages behind it are
/// still delivered.
pub async fn retryable_failure_is_redelivered<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus(GROUP).await;
    establish(&bus, kind, name_for(kind)).await;
    produce(&bus, message(kind, "flaky")).await;
    produce(&bus, message(kind, "next")).await;

    let attempts = Arc::new(AtomicUsize::new(0));
    let handled = Arc::new(Mutex::new(Vec::new()));
    let (count, done) = (attempts.clone(), handled.clone());
    let handler = move |message: &Message| {
        let id = message.id().unwrap_or_default().to_string();
        let fail = id == "flaky" && count.fetch_add(1, Ordering::SeqCst) == 0;
        if !fail {
            done.lock().unwrap().push(id);
        }
        async move {
            if fail {
                Err(TransportError::retryable("not yet"))
            } else {
                Ok(())
            }
        }
    };
    let handlers = match kind {
        MessageKind::Command => Handlers::new().on_command(name_for(kind), handler),
        MessageKind::Event => Handlers::new().on_event(name_for(kind), handler),
    };
    consume(&bus, kind, handlers, RunOptions::until_idle())
        .await
        .expect("consumer drains");

    assert_eq!(attempts.load(Ordering::SeqCst), 2, "failed once, then ran");
    let mut handled = handled.lock().unwrap().clone();
    handled.sort();
    assert_eq!(handled, vec!["flaky".to_string(), "next".to_string()]);
}

/// A permanent failure is stored on the bus as a dead letter, is not
/// delivered again, and does not hold up the messages behind it.
pub async fn permanent_failure_is_dead_lettered<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus(GROUP).await;
    let dead_letters = harness.bus(DEAD_LETTER_GROUP).await;
    establish(&bus, kind, name_for(kind)).await;
    establish(&dead_letters, MessageKind::Event, DEAD_LETTER_EVENT).await;
    produce(&bus, message(kind, "poison")).await;
    produce(&bus, message(kind, "ok")).await;

    let handled = Arc::new(Mutex::new(Vec::new()));
    let done = handled.clone();
    let handler = move |message: &Message| {
        let id = message.id().unwrap_or_default().to_string();
        let poison = id == "poison";
        if !poison {
            done.lock().unwrap().push(id);
        }
        async move {
            if poison {
                Err(TransportError::permanent("unprocessable"))
            } else {
                Ok(())
            }
        }
    };
    let handlers = || match kind {
        MessageKind::Command => Handlers::new()
            .named(GROUP)
            .on_command(name_for(kind), handler.clone()),
        MessageKind::Event => Handlers::new()
            .named(GROUP)
            .on_event(name_for(kind), handler.clone()),
    };
    consume(&bus, kind, handlers(), RunOptions::until_idle())
        .await
        .expect("consumer drains past the poison message");
    // A second run finds nothing: the poison message was acknowledged.
    consume(&bus, kind, handlers(), RunOptions::until_idle())
        .await
        .expect("second run drains");
    assert_eq!(*handled.lock().unwrap(), vec!["ok".to_string()]);

    let stored = Seen::default();
    consume(
        &dead_letters,
        MessageKind::Event,
        on(
            MessageKind::Event,
            DEAD_LETTER_EVENT,
            Handlers::new(),
            stored.clone(),
        ),
        RunOptions::until_idle(),
    )
    .await
    .expect("dead-letter reader drains");
    let stored = stored.lock().unwrap();
    assert_eq!(stored.len(), 1, "one dead letter was stored");
    let dead: DeadLetter =
        serde_json::from_slice(stored[0].payload()).expect("dead letter payload decodes");
    assert_eq!(dead.message.id(), Some("poison"));
    assert_eq!(dead.message.name(), name_for(kind));
    assert!(dead.reason.contains("unprocessable"), "{}", dead.reason);
    assert_eq!(dead.attempts, 1);
    assert_eq!(dead.consumer, GROUP, "the consumer is named");
}

/// A running consumer stays up while the transport is quiet, handles a
/// message published later, and returns `Ok` when shut down.
pub async fn quiet_consumer_runs_until_shutdown<H: BusHarness>(harness: &H, kind: MessageKind) {
    let bus = harness.bus(GROUP).await;
    let producer = harness.bus(GROUP).await;
    establish(&bus, kind, name_for(kind)).await;

    let seen = Seen::default();
    let shutdown = Shutdown::new();
    let run = consume(
        &bus,
        kind,
        on(kind, name_for(kind), Handlers::new(), seen.clone()),
        RunOptions::until_shutdown(shutdown.clone()),
    );
    let drive = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        produce(&producer, message(kind, "late")).await;
        tokio::time::timeout(Duration::from_secs(60), async {
            while seen.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the quiet consumer handled the late message");
        shutdown.trigger();
    };
    let (result, ()) = tokio::join!(run, drive);
    result.expect("shutdown ends the run cleanly");
    assert_eq!(ids(&seen), vec!["late".to_string()]);
}

/// Instantiate the bus contract for one [`BusHarness`].
#[macro_export]
macro_rules! bus_conformance_tests {
    ($harness:ty) => {
        mod bus_conformance {
            use super::*;
            use distributed::microsvc::MessageKind;
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
                retryable_command_failure_is_redelivered,
                retryable_failure_is_redelivered,
                Command
            );
            scenario!(
                retryable_event_failure_is_redelivered,
                retryable_failure_is_redelivered,
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
