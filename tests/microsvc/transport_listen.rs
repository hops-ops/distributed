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
use distributed::microsvc::{Message, MessageKind, Routes, Service, Session};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::json;

use crate::handlers;
use crate::models::counter::Counter;

fn counter_service(store: InMemoryRepository) -> Arc<Service> {
    Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_repo(store.queued().aggregate::<Counter>()),
        command handlers::counter_create,
        command handlers::counter_increment,
        command handlers::whoami,
    )))
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
    // must not wedge the consumer. NotFound is retryable, so the run gives up
    // after the first delivery and dead-letters it. (It is not retried here:
    // the sync queued `get` inside the handler holds that aggregate's lock
    // once the handler errors before committing, so a second delivery of the
    // same id would block.)
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
        RunOptions::until_idle().with_max_attempts(1),
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
    service
        .dispatch("counter.initialize", json!({ "id": "c2" }), Session::new())
        .await
        .expect("direct dispatch should create c2");

    let repo = store.queued().aggregate::<Counter>();
    let c1: Counter = repo.get("c1").await.unwrap().unwrap();
    let c2: Counter = repo.get("c2").await.unwrap().unwrap();
    assert_eq!(c1.value, 0);
    assert_eq!(c2.value, 0);
}

#[tokio::test]
async fn metadata_becomes_session() {
    let bus = InMemoryBus::new();
    let service = counter_service(InMemoryRepository::new());

    // `whoami` reads `ctx.user_id()`, which the runner derives from the message
    // metadata (`message_to_session` lowercases keys into session variables).
    bus.send_message(
        command("session.identify", "cmd-1", "{}").with_metadata("x-user-id", "user-42"),
    )
    .await
    .expect("whoami should enqueue");

    // Stop on permanent failure so a missing session user would surface as Err;
    // `whoami` succeeding proves the metadata became the session.
    bus.listen(
        service.clone(),
        RunOptions::until_idle().with_failure_policy(FailurePolicy::Stop),
    )
    .await
    .expect("whoami should succeed because metadata became the session");

    // Negative control: without metadata, whoami has no user (permanent
    // Unauthorized) and the Stop policy surfaces the failure.
    bus.send_message(command("session.identify", "cmd-2", "{}"))
        .await
        .expect("whoami should enqueue");
    let result = bus
        .listen(
            service.clone(),
            RunOptions::until_idle().with_failure_policy(FailurePolicy::Stop),
        )
        .await;
    assert!(
        result.is_err(),
        "without metadata, whoami has no session user and must fail"
    );
}

#[tokio::test]
async fn multiple_services_on_different_queues() {
    let bus = InMemoryBus::new();
    let store = InMemoryRepository::new();

    let service_a = Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_repo(store.clone().queued().aggregate::<Counter>()),
        command handlers::counter_create,
    )));
    let service_b = Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_repo(store.clone().queued().aggregate::<Counter>()),
        command handlers::counter_increment,
    )));

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
