//! The SQL bus contract, once for SQLite and Postgres.
//!
//! Both buses share their control flow in `src/bus/sql_bus_common.rs`; this
//! suite is its counterpart. A backend implements [`SqlBackend`] (construct,
//! the dialect's builder calls, raw SQL for fault injection) and calls
//! `sql_bus_conformance_tests!(Backend);`.
//!
//! Raw SQL only injects faults: a corrupt row, a lost identity row, a
//! rebuilt log, an expired claim. What the bus did about it is read back
//! through the bus: what a consumer receives, the dead letters it stores,
//! the ordering evidence it attaches. No assertion reads a bus table.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::transport::bus::BusHarness;
use distributed::bus::{
    Bus, BusConsumer, DeadLetter, Handlers, Message, MessageKind, MessageRouter, OrderedDelivery,
    RunOptions, SubscriptionPlan, TransportError, DEAD_LETTER_EVENT,
};
use distributed::projection_protocol::ProjectionEpoch;
use distributed::trace_context::{CAUSATION_ID, TRACEPARENT};
use tokio::sync::Notify;

/// A SQL bus backend. [`BusHarness::start`] opens an isolated database with
/// the bus tables created; [`BusHarness::bus`] is a bus over it.
pub trait SqlBackend: BusHarness<Bus: Clone + Send + Sync + 'static> + Send + Sync {
    /// `bus_queue` without its NOT NULL and CHECK guards, so a test can
    /// store a row the real schema refuses.
    const PERMISSIVE_QUEUE: &'static [&'static str];
    /// `bus_log` without its NOT NULL and CHECK guards.
    const PERMISSIVE_LOG: &'static [&'static str];
    /// A SQL literal of the payload `{}`.
    const PAYLOAD: &'static str;

    /// An isolated database without the bus tables.
    fn start_empty() -> impl Future<Output = Option<Self>>;
    /// Run one statement, for fault injection.
    fn execute(&self, sql: &str) -> impl Future<Output = Result<(), String>>;

    fn ensure_tables(bus: &Self::Bus) -> impl Future<Output = Result<(), TransportError>>;
    fn with_source_epoch(bus: Self::Bus, epoch: ProjectionEpoch) -> Self::Bus;
    fn with_lease(bus: Self::Bus, lease: Duration) -> Self::Bus;
    fn reset_ordered_log(
        bus: &Self::Bus,
        expected: &ProjectionEpoch,
        next: &ProjectionEpoch,
    ) -> impl Future<Output = Result<(), TransportError>>;
}

const COMMAND: &str = "sql.ship";
const EVENT: &str = "sql.shipped";
const GROUP: &str = "sql";

fn command(id: &str) -> Message {
    Message::new(COMMAND, MessageKind::Command, b"{}".to_vec()).with_id(id)
}

fn event(id: impl Into<String>) -> Message {
    Message::new(EVENT, MessageKind::Event, b"{}".to_vec()).with_id(id)
}

async fn inject<B: SqlBackend>(backend: &B, sql: &str) {
    backend
        .execute(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
}

/// The id of every message a router handles, in order.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<String>>>);

impl Seen {
    fn ids(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

fn recording(kind: MessageKind, group: &str, seen: &Seen) -> Arc<Handlers> {
    let seen = seen.clone();
    let handler = move |message: &Message| {
        seen.0
            .lock()
            .unwrap()
            .push(message.id().unwrap_or_default().to_string());
        async { Ok(()) }
    };
    let handlers = Handlers::new().named(group);
    Arc::new(match kind {
        MessageKind::Command => handlers.on_command(COMMAND, handler),
        MessageKind::Event => handlers.on_event(EVENT, handler),
    })
}

/// What `group` handles when it drains now.
async fn drain<B: Bus + BusConsumer>(bus: &B, kind: MessageKind, group: &str) -> Vec<String> {
    let seen = Seen::default();
    let router = recording(kind, group, &seen);
    match kind {
        MessageKind::Command => bus.listen(router, RunOptions::until_idle()).await,
        MessageKind::Event => bus.subscribe(router, RunOptions::until_idle()).await,
    }
    .expect("the consumer drains");
    seen.ids()
}

/// Every dead letter stored on the bus so far.
async fn dead_letters<B: Bus + BusConsumer>(bus: &B) -> Vec<DeadLetter> {
    let stored = Arc::new(Mutex::new(Vec::new()));
    let store = stored.clone();
    let handlers = Handlers::new().named("dead-letter-reader").on_event(
        DEAD_LETTER_EVENT,
        move |message: &Message| {
            store.lock().unwrap().push(
                serde_json::from_slice::<DeadLetter>(message.payload())
                    .expect("dead letter payload"),
            );
            async { Ok(()) }
        },
    );
    bus.subscribe(Arc::new(handlers), RunOptions::until_idle())
        .await
        .expect("dead letters drain");
    let stored = stored.lock().unwrap();
    stored.clone()
}

/// The ordering evidence of every event a group handles: id, epoch,
/// position.
#[derive(Default)]
struct Positions {
    group: String,
    seen: Mutex<Vec<(String, String, u64, Option<String>)>>,
}

impl Positions {
    fn named(group: &str) -> Arc<Self> {
        Arc::new(Self {
            group: group.to_string(),
            ..Self::default()
        })
    }

    /// `(id, epoch, position)` of each delivery.
    fn positions(&self) -> Vec<(String, String, u64)> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .map(|(id, epoch, position, _)| (id.clone(), epoch.clone(), *position))
            .collect()
    }
}

impl MessageRouter for Positions {
    fn consumer_group(&self) -> Option<&str> {
        Some(&self.group)
    }

    fn handles(&self, kind: MessageKind, name: &str) -> bool {
        kind == MessageKind::Event && name == EVENT
    }

    fn subscription_plan(&self) -> SubscriptionPlan {
        SubscriptionPlan {
            commands: Vec::new(),
            events: vec![EVENT.to_string()],
        }
    }

    async fn dispatch(&self, _message: &Message) -> Result<(), TransportError> {
        Err(TransportError::permanent("missing SQL ordering evidence"))
    }

    async fn dispatch_ordered(
        &self,
        message: &Message,
        ordered: Option<&OrderedDelivery>,
    ) -> Result<(), TransportError> {
        let ordered =
            ordered.ok_or_else(|| TransportError::permanent("missing SQL ordering evidence"))?;
        self.seen.lock().unwrap().push((
            message.id().unwrap_or_default().to_string(),
            ordered.epoch().as_str().to_string(),
            ordered.position(),
            message.traceparent().map(str::to_string),
        ));
        Ok(())
    }
}

async fn positions<B: Bus + BusConsumer>(bus: &B, group: &str) -> Arc<Positions> {
    let positions = Positions::named(group);
    bus.subscribe(positions.clone(), RunOptions::until_idle())
        .await
        .expect("the ordered reader drains");
    positions
}

/// The epoch the log is in now, read from the evidence of a fresh group.
async fn current_epoch<B: Bus + BusConsumer>(bus: &B, group: &str) -> ProjectionEpoch {
    let seen = positions(bus, group).await.positions();
    let (_, epoch, _) = seen.last().expect("the log holds an event");
    ProjectionEpoch::new(epoch.clone()).unwrap()
}

/// The schema refuses a message kind other than command or event.
pub async fn schema_refuses_an_unknown_kind<B: SqlBackend>(backend: B) {
    for table in ["bus_queue", "bus_log"] {
        let error = backend
            .execute(&format!(
                "INSERT INTO {table} (name, kind, payload) VALUES ('sql.x', 'bogus', {})",
                B::PAYLOAD
            ))
            .await
            .expect_err("the kind check refuses it");
        assert!(error.to_lowercase().contains("check"), "{table}: {error}");
    }
}

/// A corrupt queue row is dead-lettered, not decoded into an empty message
/// and acknowledged; the valid command beside it is handled.
pub async fn corrupt_queue_rows_are_dead_lettered<B: SqlBackend>(backend: B) {
    let bus = backend.bus().await;
    for sql in B::PERMISSIVE_QUEUE {
        inject(&backend, sql).await;
    }
    let corrupt_latest =
        "UPDATE bus_queue SET kind = 'bogus' WHERE seq = (SELECT max(seq) FROM bus_queue)";
    bus.send_message(command("poison")).await.unwrap();
    inject(&backend, corrupt_latest).await;
    bus.send_message(command("poison-kind")).await.unwrap();
    inject(&backend, corrupt_latest).await;
    bus.send_message(command("ok")).await.unwrap();

    assert_eq!(drain(&bus, MessageKind::Command, GROUP).await, vec!["ok"]);
    assert_eq!(
        drain(&bus, MessageKind::Command, GROUP).await,
        Vec::<String>::new(),
        "the corrupt rows were settled, not redelivered"
    );
    let dead = dead_letters(&bus).await;
    assert_eq!(dead.len(), 2, "every corrupt row was stored");
    for dead in dead {
        assert!(dead.reason.contains("corrupt row"), "{}", dead.reason);
        assert_eq!(dead.attempts, 1);
    }
}

/// Corrupt log entries are dead-lettered and the group's position moves
/// past them: the valid event is handled, and only later events after it.
pub async fn corrupt_log_rows_are_dead_lettered<B: SqlBackend>(backend: B) {
    let bus = backend.bus().await;
    for sql in B::PERMISSIVE_LOG {
        inject(&backend, sql).await;
    }
    let latest = "WHERE seq = (SELECT max(seq) FROM bus_log)";
    let corruptions = [
        (
            "poison",
            format!("UPDATE bus_log SET kind = 'bogus' {latest}"),
        ),
        ("ok", String::new()),
        (
            "poison-tail",
            format!("UPDATE bus_log SET kind = 'bogus' {latest}"),
        ),
        (
            "poison-kind",
            format!("UPDATE bus_log SET kind = 'bogus' {latest}"),
        ),
        (
            "poison-metadata",
            format!("UPDATE bus_log SET metadata = 'not-json' {latest}"),
        ),
        (
            "poison-content-type",
            format!("UPDATE bus_log SET content_type = NULL {latest}"),
        ),
    ];
    for (id, corruption) in corruptions {
        bus.publish_message(event(id)).await.unwrap();
        if !corruption.is_empty() {
            inject(&backend, &corruption).await;
        }
    }

    assert_eq!(drain(&bus, MessageKind::Event, GROUP).await, vec!["ok"]);
    bus.publish_message(event("after")).await.unwrap();
    assert_eq!(
        drain(&bus, MessageKind::Event, GROUP).await,
        vec!["after"],
        "the group's position moved past every corrupt entry"
    );
    assert_eq!(
        dead_letters(&bus).await.len(),
        5,
        "every corrupt entry was stored"
    );
}

/// An ambiguous publish retry with the same stable id and causal envelope is
/// stored once, at its original position, with the first envelope's trace;
/// the same id with a different payload or causation is refused.
pub async fn a_stable_id_keeps_its_first_position<B: SqlBackend>(backend: B) {
    let bus = backend.bus().await;
    bus.publish_message(
        event("stable")
            .with_metadata(CAUSATION_ID, "command-1")
            .with_metadata(TRACEPARENT, "first-attempt"),
    )
    .await
    .expect("first append commits");
    bus.publish_message(
        event("stable")
            .with_metadata(CAUSATION_ID, "command-1")
            .with_metadata(TRACEPARENT, "retry-attempt"),
    )
    .await
    .expect("the same causal envelope is an idempotent retry");

    let payload_conflict = Message::new(EVENT, MessageKind::Event, br#"{"other":1}"#.to_vec())
        .with_id("stable")
        .with_metadata(CAUSATION_ID, "command-1");
    let error = bus.publish_message(payload_conflict).await.unwrap_err();
    assert!(
        error.is_permanent(),
        "a different payload is refused: {error}"
    );
    let causation_conflict = event("stable").with_metadata(CAUSATION_ID, "other-command");
    let error = bus.publish_message(causation_conflict).await.unwrap_err();
    assert!(
        error.is_permanent(),
        "a different causation is refused: {error}"
    );

    let reader = positions(&bus, GROUP).await;
    let seen = reader.seen.lock().unwrap();
    assert_eq!(seen.len(), 1, "stored once");
    let (id, _, position, trace) = &seen[0];
    assert_eq!((id.as_str(), *position), ("stable", 1));
    assert_eq!(trace.as_deref(), Some("first-attempt"));
}

/// Two concurrent publishes of one stable envelope store it once.
pub async fn concurrent_stable_retries_store_once<B: SqlBackend>(backend: B) {
    let bus = backend.bus().await;
    let attempt = |trace: &str| {
        event("concurrent")
            .with_metadata(CAUSATION_ID, "command-concurrent")
            .with_metadata(TRACEPARENT, trace)
    };
    let (a, b) = tokio::join!(
        bus.publish_message(attempt("a")),
        bus.publish_message(attempt("b"))
    );
    a.expect("one append wins");
    b.expect("the other is an idempotent retry");

    let seen = positions(&bus, GROUP).await.positions();
    assert_eq!(seen.len(), 1);
    assert_eq!((seen[0].0.as_str(), seen[0].2), ("concurrent", 1));
}

/// A log whose identity row is lost is not given a random new epoch; an
/// operator adopts it under a named one, and every group then reads it again
/// under that epoch.
pub async fn a_lost_identity_is_adopted_only_explicitly<B: SqlBackend>(backend: B) {
    let bus = backend.bus().await;
    bus.publish_message(event("retained")).await.unwrap();
    assert_eq!(
        drain(&bus, MessageKind::Event, GROUP).await,
        vec!["retained"]
    );
    inject(&backend, "DROP TABLE bus_log_identity").await;

    let error = B::ensure_tables(&backend.bus().await)
        .await
        .expect_err("a retained log cannot receive a random replacement epoch");
    assert!(error.is_permanent());
    let error = B::ensure_tables(&backend.bus().await)
        .await
        .expect_err("the failed adoption installed nothing");
    assert!(error.is_permanent());

    let adopted = ProjectionEpoch::new("operator-adopted-retained-log").unwrap();
    let adopting = B::with_source_epoch(backend.bus().await, adopted.clone());
    B::ensure_tables(&adopting)
        .await
        .expect("explicitly adopt the retained log");

    let seen = positions(&bus, GROUP).await.positions();
    assert_eq!(
        seen,
        vec![("retained".to_string(), adopted.as_str().to_string(), 1)],
        "the group's old position was retired with the old epoch"
    );
}

/// A configured epoch names a new log; it cannot relabel an existing one.
pub async fn a_configured_epoch_cannot_relabel_a_log<B: SqlBackend>(backend: B) {
    let initial = ProjectionEpoch::new("operator-generation-1").unwrap();
    let bus = B::with_source_epoch(backend.bus().await, initial.clone());
    B::ensure_tables(&bus)
        .await
        .expect("a configured epoch initializes a new log");
    bus.publish_message(event("first")).await.unwrap();
    assert_eq!(
        positions(&bus, GROUP).await.positions(),
        vec![("first".to_string(), initial.as_str().to_string(), 1)]
    );

    let mismatched = B::with_source_epoch(
        backend.bus().await,
        ProjectionEpoch::new("operator-generation-2").unwrap(),
    );
    let error = B::ensure_tables(&mismatched).await.unwrap_err();
    assert!(error.is_permanent(), "{error}");
    let error = mismatched
        .publish_message(event("must-not-append"))
        .await
        .unwrap_err();
    assert!(error.is_permanent(), "{error}");

    assert_eq!(
        positions(&backend.bus().await, "fresh").await.positions(),
        vec![("first".to_string(), initial.as_str().to_string(), 1)],
        "the mismatched producer appended nothing"
    );
}

/// A log rebuilt underneath the bus (positions start again at 1) is refused
/// until an operator resets it, fenced on the epoch it replaces. After the
/// reset every group reads the new log from its start, under the new epoch.
pub async fn a_rewound_log_needs_a_fenced_reset<B: SqlBackend>(backend: B) {
    let bus = backend.bus().await;
    bus.publish_message(event("before")).await.unwrap();
    assert_eq!(drain(&bus, MessageKind::Event, GROUP).await, vec!["before"]);
    let before = current_epoch(&bus, "epoch-reader").await;

    inject(&backend, "DROP TABLE bus_log").await;
    let error = B::ensure_tables(&backend.bus().await).await.unwrap_err();
    assert!(
        error.is_permanent(),
        "startup cannot reuse positions: {error}"
    );
    let error = bus
        .publish_message(event("must-not-rotate"))
        .await
        .unwrap_err();
    assert!(
        error.is_permanent(),
        "publish cannot reuse positions: {error}"
    );

    let wrong = ProjectionEpoch::new("wrong-current-generation").unwrap();
    let next = ProjectionEpoch::new("operator-reset-generation-2").unwrap();
    let error = B::reset_ordered_log(&bus, &wrong, &next).await.unwrap_err();
    assert!(error.is_permanent(), "a stale expected epoch is refused");
    let error = B::reset_ordered_log(&bus, &before, &before)
        .await
        .unwrap_err();
    assert!(error.is_permanent(), "the retired epoch cannot be reused");
    B::reset_ordered_log(&bus, &before, &next)
        .await
        .expect("operator-authorized reset");

    backend
        .bus()
        .await
        .publish_message(event("after"))
        .await
        .expect("append in the new generation");
    assert_eq!(
        positions(&bus, GROUP).await.positions(),
        vec![("after".to_string(), next.as_str().to_string(), 1)],
        "the group starts the new log from its start, under the new epoch"
    );
}

/// A running subscriber whose log is reset under it stops before it
/// dispatches anything else it already read, and settles nothing into the
/// new epoch.
pub async fn a_reset_stops_a_running_subscriber<B: SqlBackend>(backend: B) {
    let producer = backend.bus().await;
    for n in 0..17 {
        producer
            .publish_message(event(format!("old-{n}")))
            .await
            .unwrap();
    }
    let epoch = current_epoch(&producer, "epoch-reader").await;

    #[derive(Default)]
    struct Blocking {
        attempts: AtomicUsize,
        seen: Mutex<Vec<String>>,
        started: Notify,
        release: Notify,
    }
    impl MessageRouter for Blocking {
        fn consumer_group(&self) -> Option<&str> {
            Some("rotation-observer")
        }
        fn handles(&self, kind: MessageKind, name: &str) -> bool {
            kind == MessageKind::Event && name == EVENT
        }
        fn subscription_plan(&self) -> SubscriptionPlan {
            SubscriptionPlan {
                commands: Vec::new(),
                events: vec![EVENT.to_string()],
            }
        }
        async fn dispatch(&self, message: &Message) -> Result<(), TransportError> {
            self.seen
                .lock()
                .unwrap()
                .push(message.id().unwrap_or_default().to_string());
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
    }

    let router = Arc::new(Blocking::default());
    let subscriber = backend.bus().await;
    let running = tokio::spawn({
        let router = router.clone();
        async move { subscriber.subscribe(router, RunOptions::until_idle()).await }
    });
    router.started.notified().await;

    let next = ProjectionEpoch::new("live-reset-generation-2").unwrap();
    B::reset_ordered_log(&producer, &epoch, &next)
        .await
        .expect("reset while a handler is in flight");
    producer.publish_message(event("new-0")).await.unwrap();
    router.release.notify_one();

    let error = running
        .await
        .expect("subscriber task joined")
        .expect_err("the retired subscriber fails closed");
    assert!(error.is_permanent(), "an epoch mismatch is not retryable");
    assert_eq!(
        *router.seen.lock().unwrap(),
        vec!["old-0".to_string()],
        "rows read before the reset were never dispatched"
    );
    assert_eq!(
        positions(&producer, "rotation-observer").await.positions(),
        vec![("new-0".to_string(), next.as_str().to_string(), 1)],
        "the retired delivery settled nothing into the new epoch"
    );
}

/// One subscriber of a group reads the log at a time: while it holds the
/// group, another subscriber of the group receives nothing, and once it
/// stops, the group's next run starts after what it acknowledged.
pub async fn one_subscriber_reads_for_a_group<B: SqlBackend>(backend: B) {
    let bus = backend.bus().await;
    for id in ["e0", "e1"] {
        bus.publish_message(event(id)).await.unwrap();
    }
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let seen = Seen::default();
    let holder = {
        let (started, release, seen) = (started.clone(), release.clone(), seen.clone());
        Handlers::new()
            .named(GROUP)
            .on_event(EVENT, move |message: &Message| {
                let id = message.id().unwrap_or_default().to_string();
                seen.0.lock().unwrap().push(id.clone());
                let (started, release) = (started.clone(), release.clone());
                async move {
                    if id == "e0" {
                        started.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                }
            })
    };
    let running = tokio::spawn({
        let bus = bus.clone();
        async move {
            bus.subscribe(Arc::new(holder), RunOptions::until_idle())
                .await
        }
    });
    started.notified().await;

    assert_eq!(
        drain(&backend.bus().await, MessageKind::Event, GROUP).await,
        Vec::<String>::new(),
        "the group is held"
    );
    release.notify_one();
    running.await.unwrap().expect("the holder drains");
    assert_eq!(seen.ids(), vec!["e0", "e1"]);

    bus.publish_message(event("e2")).await.unwrap();
    assert_eq!(
        drain(&backend.bus().await, MessageKind::Event, GROUP).await,
        vec!["e2"],
        "the holder wrote its position when it stopped"
    );
}

/// A worker that lost its claim cannot settle the row another worker has
/// since claimed.
pub async fn a_lost_claim_cannot_settle_the_new_one<B: SqlBackend>(backend: B) {
    let bus = B::with_lease(backend.bus().await, Duration::from_millis(250));
    bus.send_message(command("c1")).await.unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let first_claimed = Arc::new(Notify::new());
    let second_claimed = Arc::new(Notify::new());
    let finish_first = Arc::new(Notify::new());
    let finish_second = Arc::new(Notify::new());
    let handlers = Arc::new({
        let attempts = attempts.clone();
        let notifies = (
            first_claimed.clone(),
            second_claimed.clone(),
            finish_first.clone(),
            finish_second.clone(),
        );
        Handlers::new()
            .named(GROUP)
            .on_command(COMMAND, move |_: &Message| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let (first_claimed, second_claimed, finish_first, finish_second) = notifies.clone();
                async move {
                    match attempt {
                        0 => {
                            first_claimed.notify_one();
                            finish_first.notified().await;
                            Ok(())
                        }
                        1 => {
                            second_claimed.notify_one();
                            finish_second.notified().await;
                            Err(TransportError::retryable("released for a retry"))
                        }
                        _ => Ok(()),
                    }
                }
            })
    });
    let listen = || {
        let (bus, handlers) = (bus.clone(), handlers.clone());
        tokio::spawn(async move { bus.listen(handlers, RunOptions::until_idle()).await })
    };

    let first = listen();
    first_claimed.notified().await;
    // The first worker loses its claim, as when its renewals stop reaching
    // the database for longer than the lease.
    inject(
        &backend,
        "UPDATE bus_queue SET locked_until = NULL, claim_token = NULL",
    )
    .await;
    let second = listen();
    second_claimed.notified().await;

    finish_first.notify_one();
    first.await.unwrap().expect("the stale worker drains");
    finish_second.notify_one();
    second.await.unwrap().expect("the second worker drains");

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "the stale ack did not delete the newer claim before it was retried"
    );
    assert_eq!(
        drain(&bus, MessageKind::Command, GROUP).await,
        Vec::<String>::new(),
        "the retried command was acknowledged"
    );
}

/// A handler that runs longer than the claim lease keeps its claim: the
/// lease is renewed while it runs, so no second worker runs the same row.
///
/// The lease runs on the database's clock, which a test cannot drive, so
/// once the claim would have expired unrenewed, a second worker keeps
/// asking for the row. Renewals are client-driven: a holder stalled for
/// longer than the lease minus the renewal interval loses its claim, and
/// the second worker here shares the holder's process and its stalls. The
/// lease is sized for that: renewals every 3 s keep a 9 s lease through a
/// stall or a renewal of up to 6 s (4.5 s if every renewal is that slow).
/// On a loaded development machine single renewals were measured landing
/// up to 2 s late, which a 2 s lease (1.3 s of slack) did not survive. The
/// schedule itself is pinned on paused time by `keep_renewing`'s tests.
pub async fn a_long_handler_keeps_its_claim<B: SqlBackend>(backend: B) {
    let lease = Duration::from_secs(9);
    let bus = B::with_lease(backend.bus().await, lease);
    bus.send_message(command("c1")).await.unwrap();

    let attempts = Arc::new(AtomicUsize::new(0));
    let claimed = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let handlers = Arc::new({
        let (attempts, claimed, finish) = (attempts.clone(), claimed.clone(), finish.clone());
        Handlers::new()
            .named(GROUP)
            .on_command(COMMAND, move |_: &Message| {
                // Only the first run blocks: a second worker that got the row
                // shows up in `attempts` instead of hanging the test.
                let first = attempts.fetch_add(1, Ordering::SeqCst) == 0;
                let (claimed, finish) = (claimed.clone(), finish.clone());
                async move {
                    if first {
                        claimed.notify_one();
                        finish.notified().await;
                    }
                    Ok(())
                }
            })
    });
    let mut first = tokio::spawn({
        let (bus, handlers) = (bus.clone(), handlers.clone());
        async move { bus.listen(handlers, RunOptions::until_idle()).await }
    });
    tokio::select! {
        () = claimed.notified() => {}
        stopped = &mut first => panic!("the first worker stopped before it handled the row: {stopped:?}"),
    }

    // Unrenewed, the claim would have expired by now.
    tokio::time::sleep(lease).await;
    let until = tokio::time::Instant::now() + Duration::from_millis(1500);
    while tokio::time::Instant::now() < until {
        bus.listen(handlers.clone(), RunOptions::until_idle())
            .await
            .expect("a second worker finds nothing to claim");
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "no second worker ran it"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    finish.notify_one();
    first.await.unwrap().expect("the first worker drains");
    assert_eq!(
        drain(&bus, MessageKind::Command, GROUP).await,
        Vec::<String>::new(),
        "the long handler's ack settled its row"
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// Instantiate the SQL bus contract for one [`SqlBackend`].
macro_rules! sql_bus_conformance_tests {
    ($backend:ty) => {
        mod sql_bus_conformance {
            use super::*;
            use crate::sql_bus_conformance::{self as suite, SqlBackend};
            use common::transport::bus::BusHarness;

            macro_rules! case {
                ($name:ident, $start:ident) => {
                    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
                    async fn $name() {
                        let Some(backend) = <$backend>::$start().await else {
                            return;
                        };
                        tokio::time::timeout(
                            std::time::Duration::from_secs(120),
                            suite::$name(backend),
                        )
                        .await
                        .expect("the case finished");
                    }
                };
            }

            case!(schema_refuses_an_unknown_kind, start);
            case!(corrupt_queue_rows_are_dead_lettered, start);
            case!(corrupt_log_rows_are_dead_lettered, start);
            case!(a_stable_id_keeps_its_first_position, start);
            case!(concurrent_stable_retries_store_once, start);
            case!(a_lost_identity_is_adopted_only_explicitly, start);
            case!(a_configured_epoch_cannot_relabel_a_log, start_empty);
            case!(a_rewound_log_needs_a_fenced_reset, start);
            case!(a_reset_stops_a_running_subscriber, start);
            case!(one_subscriber_reads_for_a_group, start);
            case!(a_lost_claim_cannot_settle_the_new_one, start);
            case!(a_long_handler_keeps_its_claim, start);
        }
    };
}
pub(crate) use sql_bus_conformance_tests;
