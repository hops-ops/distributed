//! Tests demonstrating the handler file convention.
//!
//! Each handler lives in its own file under `handlers/` and exports:
//! - `COMMAND: &str` — the command name
//! - `command()` — the typed declaration: input, outcome and roles
//! - `handle(ctx, input)` — the handler
//!
//! Registration is `.typed_command(module::command()).handle(module::handle)`.

use distributed::microsvc::{CausalDispatchError, Routes, Service};
use distributed::{AggregateBuilder, InMemoryRepository, OutboxStore, Queueable};
use serde_json::json;

use crate::commands::dispatch;
use crate::handlers::{self, user_session};
use crate::models::counter::Counter;

// ============================================================================
// Handler convention — register, dispatch, verify
// ============================================================================

#[tokio::test]
async fn handler_modules_register_and_dispatch() {
    let store = InMemoryRepository::new();
    let service = Service::new().named("counters").routes(
        Routes::new()
            .with_repo(store.clone().queued().aggregate::<Counter>())
            .typed_command(handlers::counter_create::command())
            .handle(handlers::counter_create::handle)
            .typed_command(handlers::counter_increment::command())
            .handle(handlers::counter_increment::handle),
    );

    let mut cmds = service.command_names();
    cmds.sort();
    assert_eq!(cmds, vec!["counter.increment", "counter.initialize"]);

    // Create
    let result = dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        user_session("user-1"),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({ "id": "c1" }));

    // Increment
    let result = dispatch(
        &service,
        "counter.increment",
        json!({ "id": "c1", "amount": 10 }),
        user_session("user-1"),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({ "id": "c1", "value": 10 }));

    // Verify state via repo
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
async fn bad_input_is_refused_before_the_handler() {
    let service = Service::new().named("counters").routes(
        Routes::new()
            .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
            .typed_command(handlers::counter_create::command())
            .handle(handlers::counter_create::handle),
    );

    let result = dispatch(
        &service,
        "counter.initialize",
        json!({ "wrong": 1 }),
        user_session("user-1"),
    )
    .await;
    assert!(
        matches!(result, Err(CausalDispatchError::BadRequest(_))),
        "{result:?}"
    );
}

#[tokio::test]
async fn handler_rejects_duplicate_create() {
    let service = Service::new().named("counters").routes(
        Routes::new()
            .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
            .typed_command(handlers::counter_create::command())
            .handle(handlers::counter_create::handle),
    );

    dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        user_session("user-1"),
    )
    .await
    .unwrap();

    let result = dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        user_session("user-1"),
    )
    .await;
    assert!(
        matches!(
            result,
            Err(CausalDispatchError::Rejected { status: 422, .. })
        ),
        "{result:?}"
    );
}

// ============================================================================
// Outbox — handlers commit aggregate + outbox message atomically
// ============================================================================

#[tokio::test]
async fn create_persists_outbox_message() {
    let store = InMemoryRepository::new();
    let service = Service::new().named("counters").routes(
        Routes::new()
            .with_repo(store.clone().queued().aggregate::<Counter>())
            .typed_command(handlers::counter_create::command())
            .handle(handlers::counter_create::handle),
    );

    let result = dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        user_session("user-1"),
    )
    .await
    .unwrap();
    assert_eq!(result, json!({ "id": "c1" }));

    // Aggregate was persisted
    let counter: Counter = store
        .clone()
        .queued()
        .aggregate::<Counter>()
        .get("c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter.value, 0);

    // Outbox message was persisted
    let pending = store.outbox_store().pending(usize::MAX).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_type, "counter.initialized");
}

#[tokio::test]
async fn duplicate_create_leaves_single_outbox_message() {
    let store = InMemoryRepository::new();
    let service = Service::new().named("counters").routes(
        Routes::new()
            .with_repo(store.clone().queued().aggregate::<Counter>())
            .typed_command(handlers::counter_create::command())
            .handle(handlers::counter_create::handle),
    );

    dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        user_session("user-1"),
    )
    .await
    .unwrap();

    // Second create fails — no duplicate outbox message
    let result = dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        user_session("user-1"),
    )
    .await;
    assert!(
        matches!(
            result,
            Err(CausalDispatchError::Rejected { status: 422, .. })
        ),
        "{result:?}"
    );

    let pending = store.outbox_store().pending(usize::MAX).await.unwrap();
    assert_eq!(pending.len(), 1);
}

#[tokio::test]
async fn increment_persists_outbox_message() {
    let store = InMemoryRepository::new();
    let service = Service::new().named("counters").routes(
        Routes::new()
            .with_repo(store.clone().queued().aggregate::<Counter>())
            .typed_command(handlers::counter_create::command())
            .handle(handlers::counter_create::handle)
            .typed_command(handlers::counter_increment::command())
            .handle(handlers::counter_increment::handle),
    );

    dispatch(
        &service,
        "counter.initialize",
        json!({ "id": "c1" }),
        user_session("user-1"),
    )
    .await
    .unwrap();

    dispatch(
        &service,
        "counter.increment",
        json!({ "id": "c1", "amount": 7 }),
        user_session("user-1"),
    )
    .await
    .unwrap();

    // Aggregate state is correct
    let counter: Counter = store
        .clone()
        .queued()
        .aggregate::<Counter>()
        .get("c1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter.value, 7);

    // Both outbox messages were persisted
    let pending = store.outbox_store().pending(usize::MAX).await.unwrap();
    assert_eq!(pending.len(), 2);
    let mut event_types: Vec<&str> = pending.iter().map(|m| m.event_type.as_str()).collect();
    event_types.sort();
    assert_eq!(
        event_types,
        vec!["counter.incremented", "counter.initialized"]
    );
}
