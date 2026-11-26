//! Bus transport tests — listen (point-to-point queue consumption), over the
//! async `InMemoryBus`.
//!
//! Commands are sent to the bus and drained into a `Service` via `listen`
//! (competing-consumer queues keyed by command name). These mirror the former
//! legacy-bus `microsvc::listen` tests: the old `stats.handled`/`stats.failed`
//! handle has no async analogue, so success is asserted through domain outcomes
//! (committed aggregate state) and failures through the run's `FailurePolicy`.

use std::sync::Arc;

use distributed::bus::{Bus, BusConsumer, FailurePolicy, InMemoryBus, RunOptions};
use distributed::bus::{Message, MessageKind};
use distributed::microsvc::{Routes, Service, TRANSPORT_ROLE};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::json;

use crate::microsvc::commands::dispatch;
use crate::microsvc::handlers::{self, user_session};
use crate::microsvc::models::counter::Counter;

fn counter_service(store: InMemoryRepository) -> Arc<Service> {
    Arc::new(
        Service::new().named("counters").routes(
            Routes::new()
                .with_repo(store.queued().aggregate::<Counter>())
                .typed_command(handlers::counter_create::command())
                .handle(handlers::counter_create::handle)
                .typed_command(handlers::counter_increment::command())
                .handle(handlers::counter_increment::handle)
                // `whoami` declares no role; a bus delivers only commands
                // that opt in.
                .typed_command(handlers::whoami::command().roles([TRANSPORT_ROLE]))
                .handle(handlers::whoami::handle),
        ),
    )
}

fn command(name: &str, id: &str, payload: &str) -> Message {
    Message::new(name, MessageKind::Command, payload.as_bytes().to_vec()).with_id(id)
}

#[tokio::test]
async fn dispatches_from_queue() {
    let bus = InMemoryBus::new();
    let store = InMemoryRepository::new();
    let service = counter_service(store.clone());

    bus.send_message(command("counter.initialize", "cmd-1", r#"{"id":"c1"}"#))
        .await
        .expect("create should enqueue");
    bus.send_message(command(
        "counter.increment",
        "cmd-2",
        r#"{"id":"c1","amount":10}"#,
    ))
    .await
    .expect("increment should enqueue");

    // Per-command queues drain in registration order (create before increment).
    bus.listen(service.clone(), RunOptions::until_idle())
        .await
        .expect("listen should drain the command queues");

    let counter: Counter = store
        .queued()
        .aggregate::<Counter>()
        .get("c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter.value, 10);
}

#[tokio::test]
async fn tolerates_handler_failures_and_keeps_processing() {
    let bus = InMemoryBus::new();
    let store = InMemoryRepository::new();
    let service = counter_service(store.clone());

    // A failing message (increment a counter that was never created → NotFound)
    // must not wedge the consumer. The ledger records the refusal, so the
    // delivery fails permanently and is dead-lettered.
    bus.send_message(command(
        "counter.increment",
        "counter.rejected",
        r#"{"id":"nonexistent","amount":1}"#,
    ))
    .await
    .expect("bad increment should enqueue");
    bus.send_message(command(
        "counter.initialize",
        "good-create",
        r#"{"id":"c2"}"#,
    ))
    .await
    .expect("create should enqueue");
    bus.send_message(command(
        "counter.increment",
        "good-inc",
        r#"{"id":"c2","amount":7}"#,
    ))
    .await
    .expect("good increment should enqueue");

    bus.listen(
        service.clone(),
        RunOptions::until_idle().with_max_attempts(3),
    )
    .await
    .expect("listen should drain past the failed message");

    // The good aggregate was still created and incremented, proving the failure
    // did not stop the consumer.
    let c2: Counter = store
        .queued()
        .aggregate::<Counter>()
        .get("c2")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c2.value, 7);
}

#[tokio::test]
async fn coexists_with_direct_dispatch() {
    let bus = InMemoryBus::new();
    let store = InMemoryRepository::new();
    let service = counter_service(store.clone());

    // c1 created via the bus.
    bus.send_message(command("counter.initialize", "cmd-1", r#"{"id":"c1"}"#))
        .await
        .expect("create should enqueue");
    bus.listen(service.clone(), RunOptions::until_idle())
        .await
        .expect("listen should drain c1's create");

    // c2 created via direct dispatch on the same service.
    dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c2" }),
        user_session("user-1"),
    )
    .await
    .expect("direct dispatch should create c2");

    let repo = store.queued().aggregate::<Counter>();
    let c1: Counter = repo.get("c1").await.unwrap().unwrap();
    let c2: Counter = repo.get("c2").await.unwrap().unwrap();
    assert_eq!(c1.value, 0);
    assert_eq!(c2.value, 0);
}

#[tokio::test]
async fn bus_commands_run_as_the_transport_identity() {
    let bus = InMemoryBus::new();
    let service = counter_service(InMemoryRepository::new());

    // `whoami` reads `ctx.user_id()`. A bus authenticates nobody, so the
    // command runs as the transport identity whatever the message says.
    // Stop on permanent failure so a session without a user would surface.
    bus.send_message(command("session.identify", "cmd-1", "{}"))
        .await
        .expect("whoami should enqueue");
    bus.listen(
        service.clone(),
        RunOptions::until_idle().with_failure_policy(FailurePolicy::Stop),
    )
    .await
    .expect("whoami should succeed as the transport identity");

    // Negative control: a command that does not declare the transport role
    // is refused, whatever identity the message's metadata claims.
    let users_only = Arc::new(
        Service::new().named("counters").routes(
            Routes::new()
                .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
                .typed_command(handlers::whoami::command().roles([handlers::USER_ROLE]))
                .handle(handlers::whoami::handle),
        ),
    );
    bus.send_message(
        command("session.identify", "cmd-2", "{}")
            .with_metadata("x-user-id", "user-42")
            .with_metadata("x-roles", handlers::USER_ROLE),
    )
    .await
    .expect("whoami should enqueue");
    let result = bus
        .listen(
            users_only,
            RunOptions::until_idle().with_failure_policy(FailurePolicy::Stop),
        )
        .await;
    assert!(
        result.is_err(),
        "a command that does not declare the transport role must be refused"
    );
}

#[tokio::test]
async fn multiple_services_on_different_queues() {
    let bus = InMemoryBus::new();
    let store = InMemoryRepository::new();

    let service_a = Arc::new(
        Service::new().named("counter-create").routes(
            Routes::new()
                .with_repo(store.clone().queued().aggregate::<Counter>())
                .typed_command(handlers::counter_create::command())
                .handle(handlers::counter_create::handle),
        ),
    );
    let service_b = Arc::new(
        Service::new().named("counter-increment").routes(
            Routes::new()
                .with_repo(store.clone().queued().aggregate::<Counter>())
                .typed_command(handlers::counter_increment::command())
                .handle(handlers::counter_increment::handle),
        ),
    );

    // Each service drains only its own command queue from the shared bus, so the
    // two never compete: service_a takes `counter.initialize`, service_b takes
    // `counter.increment`.
    bus.send_message(command("counter.initialize", "cmd-1", r#"{"id":"c1"}"#))
        .await
        .expect("create should enqueue");
    bus.listen(service_a.clone(), RunOptions::until_idle())
        .await
        .expect("service A should drain the create queue");

    bus.send_message(command(
        "counter.increment",
        "cmd-2",
        r#"{"id":"c1","amount":42}"#,
    ))
    .await
    .expect("increment should enqueue");
    bus.listen(service_b.clone(), RunOptions::until_idle())
        .await
        .expect("service B should drain the increment queue");

    let counter: Counter = store
        .queued()
        .aggregate::<Counter>()
        .get("c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter.value, 42);
}
