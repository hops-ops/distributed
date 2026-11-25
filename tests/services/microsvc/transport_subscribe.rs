//! Bus transport tests — subscribe (pub/sub fan-out), over the async `InMemoryBus`.
//!
//! Events are published to the bus and drained into a subscribed `Service`,
//! proving the async bus facade dispatches pub/sub events end-to-end.

use std::sync::Arc;

use distributed::bus::{Bus, BusConsumer, InMemoryBus, RunOptions};
use distributed::microsvc::{Context, HandlerError, Message, MessageKind, Routes, Service};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::{json, Value};

use crate::microsvc::handlers::{counter_state_message, Repo};
use crate::microsvc::models::counter::{Counter, CreateCounter, IncrementCounter};

const INITIALIZE: &str = "counter.initialize";
const INCREMENT: &str = "counter.increment";

async fn on_initialize(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<CreateCounter>()?;
    if ctx.repo().get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "counter {} already exists",
            input.id
        )));
    }
    let mut counter = Counter::default();
    counter.create(input.id.clone())?;
    let message = counter_state_message(&counter, "counter.initialized")?;
    ctx.repo().outbox(message).commit(&mut counter).await?;
    Ok(json!({ "id": input.id }))
}

async fn on_increment(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let input = ctx.input::<IncrementCounter>()?;
    let mut counter: Counter = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    counter.increment(input.amount)?;
    let message = counter_state_message(&counter, "counter.incremented")?;
    ctx.repo().outbox(message).commit(&mut counter).await?;
    Ok(json!({ "id": input.id, "value": counter.value }))
}

fn counter_service(store: InMemoryRepository) -> Arc<Service> {
    Arc::new(
        Service::new().routes(
            Routes::new()
                .with_repo(store.queued().aggregate::<Counter>())
                .event(INITIALIZE)
                .guarded(|ctx| ctx.has_fields(&["id"]), on_initialize)
                .event(INCREMENT)
                .guarded(|ctx| ctx.has_fields(&["id", "amount"]), on_increment),
        ),
    )
}

#[tokio::test]
async fn dispatches_from_pubsub() {
    let bus = InMemoryBus::new();
    let store = InMemoryRepository::new();
    let service = counter_service(store.clone());

    for (id, name, payload) in [
        ("evt-1", INITIALIZE, r#"{"id":"c1"}"#),
        ("evt-2", INCREMENT, r#"{"id":"c1","amount":10}"#),
        ("evt-3", INCREMENT, r#"{"id":"c1","amount":5}"#),
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
