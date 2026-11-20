//! Bus transport tests — subscribe (pub/sub fan-out), over the async `InMemoryBus`.
//!
//! Events are published to the bus and drained into a subscribed `Service`,
//! proving the async bus facade dispatches pub/sub events end-to-end.

use std::sync::Arc;

use distributed::bus::{Bus, BusConsumer, InMemoryBus, RunOptions};
use distributed::microsvc::{Message, MessageKind, Routes, Service};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};

use crate::handlers;
use crate::models::counter::Counter;

fn counter_service(store: InMemoryRepository) -> Arc<Service> {
    Arc::new(
        Service::new().routes(
            Routes::new()
                .with_repo(store.queued().aggregate::<Counter>())
                .event(handlers::counter_create::COMMAND)
                .guarded(
                    handlers::counter_create::guard,
                    handlers::counter_create::handle,
                )
                .event(handlers::counter_increment::COMMAND)
                .guarded(
                    handlers::counter_increment::guard,
                    handlers::counter_increment::handle,
                ),
        ),
    )
}

#[tokio::test]
async fn dispatches_from_pubsub() {
    let bus = InMemoryBus::new();
    let store = InMemoryRepository::new();
    let service = counter_service(store.clone());

    for (id, name, payload) in [
        ("evt-1", handlers::counter_create::COMMAND, r#"{"id":"c1"}"#),
        (
            "evt-2",
            handlers::counter_increment::COMMAND,
            r#"{"id":"c1","amount":10}"#,
        ),
        (
            "evt-3",
            handlers::counter_increment::COMMAND,
            r#"{"id":"c1","amount":5}"#,
        ),
    ] {
        bus.publish_message(
            Message::new(name, MessageKind::Event, payload.as_bytes().to_vec()).with_id(id),
        )
        .await
        .expect("event should publish");
    }

    // Drain the published events into the subscriber (create, then the increments).
    bus.subscribe(service.clone(), RunOptions::until_idle())
        .await
        .expect("subscriber should drain the bus");

    let counter: Counter = store
        .queued()
        .aggregate::<Counter>()
        .get("c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter.value, 15);
}
