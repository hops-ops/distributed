//! Basic microsvc integration tests — exercises dispatch with a real repository.

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, Context, HandlerError, Routes, Service, Session,
};
use distributed::{AggregateBuilder, InMemoryRepository};
use serde::Serialize;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::microsvc::commands::dispatch;
use crate::microsvc::models::counter::{
    Counter, CreateCounter, DecrementCounter, IncrementCounter,
};

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct Created {
    id: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct CounterValue {
    value: i64,
}

async fn initialize(
    ctx: &CausalCommandContext<'_, Counter>,
    input: CreateCounter,
) -> Result<PreparedCommand<Succeeded<Created>>, HandlerError> {
    let mut counter = ctx.repo().create();
    counter.create(input.id.clone())?;
    ctx.repo()
        .commit(counter)?
        .succeeded(Created { id: input.id })
}

async fn increment(
    ctx: &CausalCommandContext<'_, Counter>,
    input: IncrementCounter,
) -> Result<PreparedCommand<Succeeded<CounterValue>>, HandlerError> {
    let mut counter = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    counter.increment(input.amount)?;
    let value = counter.value;
    ctx.repo()
        .commit(counter)?
        .succeeded(CounterValue { value })
}

async fn decrement(
    ctx: &CausalCommandContext<'_, Counter>,
    input: DecrementCounter,
) -> Result<PreparedCommand<Succeeded<CounterValue>>, HandlerError> {
    let mut counter = ctx
        .repo()
        .get(&input.id)
        .await?
        .ok_or_else(|| HandlerError::NotFound(input.id.clone()))?;
    counter.decrement(input.amount)?;
    let value = counter.value;
    ctx.repo()
        .commit(counter)?
        .succeeded(CounterValue { value })
}

#[tokio::test]
async fn full_lifecycle() {
    let repo = InMemoryRepository::new();
    let counter_repo = repo.clone().aggregate::<Counter>();
    let service = Service::new().named("counters").routes(
        Routes::new()
            .with_repo(repo.aggregate::<Counter>())
            .typed_command(typed_command::<CreateCounter, Succeeded<Created>>(
                "counter.initialize",
            ))
            .handle(initialize)
            .typed_command(typed_command::<IncrementCounter, Succeeded<CounterValue>>(
                "counter.increment",
            ))
            .handle(increment)
            .typed_command(typed_command::<DecrementCounter, Succeeded<CounterValue>>(
                "counter.decrement",
            ))
            .handle(decrement),
    );

    // Create
    let result = dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        Session::new(),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({ "id": "c1" }));

    // Increment twice
    let result = dispatch(
        &service,
        "counter.increment",
        json!({ "id": "c1", "amount": 5 }),
        Session::new(),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({ "value": 5 }));

    dispatch(
        &service,
        "counter.increment",
        json!({ "id": "c1", "amount": 3 }),
        Session::new(),
    )
    .await
    .unwrap();

    // Decrement
    let result = dispatch(
        &service,
        "counter.decrement",
        json!({ "id": "c1", "amount": 2 }),
        Session::new(),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({ "value": 6 }));

    // Verify final state via repo
    let counter: Counter = counter_repo.get("c1").await.unwrap().unwrap();
    assert_eq!(counter.value, 6);
}

#[tokio::test]
async fn domain_event_fans_out_to_multiple_subscribers() {
    let projections = Arc::new(AtomicUsize::new(0));
    let policies = Arc::new(AtomicUsize::new(0));
    let projection_handler_count = projections.clone();
    let policy_handler_count = policies.clone();
    let service = Service::new()
        .routes(
            Routes::new()
                .with_dependencies(projections.clone())
                .event("metachangeset.opened")
                .handle(move |_ctx: &Context<Arc<AtomicUsize>>| {
                    let count = projection_handler_count.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({ "subscriber": "projection" }))
                    }
                }),
        )
        .routes(
            Routes::new()
                .with_dependencies(policies.clone())
                .event("metachangeset.opened")
                .handle(move |_ctx: &Context<Arc<AtomicUsize>>| {
                    let count = policy_handler_count.clone();
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({ "subscriber": "policy" }))
                    }
                }),
        );

    let message = distributed::microsvc::Message::new(
        "metachangeset.opened",
        distributed::microsvc::MessageKind::Event,
        b"{}".to_vec(),
    );
    service.dispatch_message(&message).await.unwrap();
    assert_eq!(projections.load(Ordering::SeqCst), 1);
    assert_eq!(policies.load(Ordering::SeqCst), 1);
}
