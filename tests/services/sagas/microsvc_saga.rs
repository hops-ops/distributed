//! Microservice Saga Tests
//!
//! Demonstrates handler files organized by service domain under
//! `handlers/`: typed commands (`command()` + `handle`) and, for the saga,
//! event handlers registered with `routes!`.
//!
//! Each route bundle is typed to a specific aggregate via
//! `Routes::new().with_repo(repo.queued().aggregate::<T>())`, so handlers access
//! `ctx.repo().get()`, `ctx.repo().commit()`, etc. directly while `Service`
//! remains a non-generic deployment router. The commands the saga sends to
//! the other services declare the transport role: a bus delivers them, and
//! the command ledger deduplicates on the message id.
//!
//! Two tests:
//! 1. **Orchestrated** — test runner dispatches commands to each service
//! 2. **Distributed** — services communicate over the async `InMemoryBus`

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use distributed::bus::{Bus, BusConsumer, InMemoryBus, RunOptions};
use distributed::bus::{Message, MessageKind};
use distributed::microsvc::{Routes, Service, Session};
use distributed::outbox_worker::{ClaimOutboxMessages, OutboxClaimRef, OutboxStore};
use distributed::{AggregateBuilder, InMemoryOutboxStore, InMemoryRepository, Queueable};

use super::commands::dispatch;
use super::handlers;
use super::order::{Inventory, Order, OrderFulfillmentSaga, OrderStatus, Payment, SagaStatus};

/// A command as a bus delivers it: a message with an id, which the command
/// ledger deduplicates on.
fn command_message(name: &str, input: serde_json::Value) -> Message {
    Message::new(
        name,
        MessageKind::Command,
        serde_json::to_vec(&input).unwrap(),
    )
    .with_id(format!("{name}:test-runner"))
}

fn event_message(name: &str, input: serde_json::Value) -> Message {
    Message::new(
        name,
        MessageKind::Event,
        serde_json::to_vec(&input).unwrap(),
    )
}

// ============================================================================
// Test 1: Orchestrated — test runner dispatches to each service in sequence
// ============================================================================

/// Test runner plays the role of the message bus, delivering each command
/// in the correct order.  Outbox messages are created but ignored (no workers).
///
/// ```text
///  ┌────────────────────────────────────────────────────────────────┐
///  │                    Test Runner (orchestrator)                   │
///  │                                                                │
///  │  dispatch ──→ [Saga Service]      typed: OrderFulfillmentSaga  │
///  │  dispatch ──→ [Order Service]     typed: Order                 │
///  │  dispatch ──→ [Inventory Service] typed: Inventory             │
///  │  dispatch ──→ [Payment Service]   typed: Payment               │
///  └────────────────────────────────────────────────────────────────┘
/// ```
#[tokio::test]
async fn saga_orchestrated() {
    let saga_repo = InMemoryRepository::new();
    let saga_svc = Service::new().named("saga").routes(distributed::routes!(
        Routes::new()
            .with_repo(saga_repo.clone().queued().aggregate::<OrderFulfillmentSaga>())
            .typed_command(handlers::saga::start::command())
            .handle(handlers::saga::start::handle),
        event handlers::saga::on_order_created,
        event handlers::saga::on_inventory_reserved,
        event handlers::saga::on_payment_succeeded,
        event handlers::saga::on_order_completed,
    ));

    let order_repo = InMemoryRepository::new();
    let order_svc = Service::new().named("orders").routes(
        Routes::new()
            .with_repo(order_repo.clone().queued().aggregate::<Order>())
            .typed_command(handlers::orders::create::command())
            .handle(handlers::orders::create::handle)
            .typed_command(handlers::orders::complete::command())
            .handle(handlers::orders::complete::handle),
    );

    let inventory_repo = InMemoryRepository::new();
    let inventory_svc = Service::new().named("inventory").routes(
        Routes::new()
            .with_repo(inventory_repo.clone().queued().aggregate::<Inventory>())
            .typed_command(handlers::inventory::init::command())
            .handle(handlers::inventory::init::handle)
            .typed_command(handlers::inventory::reserve::command())
            .handle(handlers::inventory::reserve::handle),
    );

    let payment_repo = InMemoryRepository::new();
    let payment_svc = Service::new().named("payments").routes(
        Routes::new()
            .with_repo(payment_repo.clone().queued().aggregate::<Payment>())
            .typed_command(handlers::payments::process::command())
            .handle(handlers::payments::process::handle),
    );

    // 1. Initialize inventory
    dispatch(
        &inventory_svc,
        "inventory.initialize",
        json!({ "sku": "WIDGET-001", "stock": 100 }),
        Session::new(),
    )
    .await
    .unwrap();

    // 2. Start saga → creates saga + outbox(CreateOrder)
    dispatch(
        &saga_svc,
        "saga.start",
        json!({
            "saga_id": "saga-001",
            "order_id": "order-001",
            "customer_id": "customer-001",
            "items": [{ "sku": "WIDGET-001", "quantity": 5, "price_cents": 1000 }],
            "total_cents": 5000
        }),
        Session::new(),
    )
    .await
    .unwrap();

    // 3. Create order -> outbox(order.initialized)
    order_svc
        .dispatch_message(&command_message(
            "order.initialize",
            json!({
                "saga_id": "saga-001",
                "order_id": "order-001",
                "customer_id": "customer-001",
                "items": [{ "sku": "WIDGET-001", "quantity": 5, "price_cents": 1000 }],
                "total_cents": 5000
            }),
        ))
        .await
        .unwrap();

    // 4. Saga: order created → outbox(ReserveInventory)
    saga_svc
        .dispatch_message(&event_message(
            "order.initialized",
            json!({ "saga_id": "saga-001", "order_id": "order-001" }),
        ))
        .await
        .unwrap();

    // 5. Reserve inventory → outbox(InventoryReserved)
    inventory_svc
        .dispatch_message(&command_message(
            "inventory.reserve",
            json!({
                "saga_id": "saga-001",
                "order_id": "order-001",
                "sku": "WIDGET-001",
                "quantity": 5
            }),
        ))
        .await
        .unwrap();

    // 6. Saga: inventory reserved → outbox(ProcessPayment)
    saga_svc
        .dispatch_message(&event_message(
            "inventory.reserved",
            json!({ "saga_id": "saga-001", "order_id": "order-001" }),
        ))
        .await
        .unwrap();

    // 7. Process payment → outbox(PaymentSucceeded)
    payment_svc
        .dispatch_message(&command_message(
            "payment.process",
            json!({
                "saga_id": "saga-001",
                "order_id": "order-001",
                "amount_cents": 5000
            }),
        ))
        .await
        .unwrap();

    // 8. Saga: payment succeeded → outbox(CompleteOrder)
    saga_svc
        .dispatch_message(&event_message(
            "payment.succeeded",
            json!({ "saga_id": "saga-001", "order_id": "order-001" }),
        ))
        .await
        .unwrap();

    // 9. Complete order → outbox(OrderCompleted)
    order_svc
        .dispatch_message(&command_message(
            "order.complete",
            json!({ "saga_id": "saga-001", "order_id": "order-001" }),
        ))
        .await
        .unwrap();

    // 10. Saga: order completed → saga done
    saga_svc
        .dispatch_message(&event_message(
            "order.completed",
            json!({ "saga_id": "saga-001", "order_id": "order-001" }),
        ))
        .await
        .unwrap();

    // === Verify final state — typed repos return aggregates directly ===

    let saga = saga_repo
        .queued()
        .aggregate::<OrderFulfillmentSaga>()
        .peek("saga-001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saga.status(), SagaStatus::Completed);
    assert!(saga.is_complete());

    let order = order_repo
        .queued()
        .aggregate::<Order>()
        .peek("order-001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(order.status(), OrderStatus::Completed);

    let inv = inventory_repo
        .queued()
        .aggregate::<Inventory>()
        .peek("WIDGET-001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inv.available(), 95);

    let payment = payment_repo
        .queued()
        .aggregate::<Payment>()
        .peek("pay-order-001")
        .await
        .unwrap()
        .unwrap();
    assert!(payment.is_successful());
}

// ============================================================================
// Test 2: Distributed — services communicate over the async InMemoryBus
// ============================================================================

/// Drain a service's outbox onto the bus, routing by destination: messages
/// addressed to a worker service (`destination != "saga"`) are point-to-point
/// commands (consumed via `listen`); messages addressed to the saga
/// (`destination == "saga"`) are events (consumed via `subscribe`). Returns how
/// many messages were forwarded.
async fn publish_pending_outbox(outbox: &InMemoryOutboxStore, bus: &InMemoryBus) -> usize {
    let claimed = outbox
        .claim(ClaimOutboxMessages::new(
            "saga-outbox-bridge",
            64,
            Duration::from_secs(60),
        ))
        .await
        .expect("outbox claim should succeed");
    let count = claimed.len();
    for message in claimed {
        let is_event = message.destination.as_deref() == Some("saga");
        let kind = if is_event {
            MessageKind::Event
        } else {
            MessageKind::Command
        };
        let bus_message = Message::new(message.event_type.clone(), kind, message.payload.clone())
            .with_id(message.id().to_string());
        if is_event {
            bus.publish_message(bus_message)
                .await
                .expect("saga event should publish");
        } else {
            bus.send_message(bus_message)
                .await
                .expect("service command should enqueue");
        }
        let claim = OutboxClaimRef::from_message(&message).expect("claimed message yields a ref");
        outbox
            .complete(&claim)
            .await
            .expect("forwarded message should complete");
    }
    count
}

/// Each service owns its aggregate + outbox; the choreography flows entirely
/// over the bus. Instead of threaded outbox workers + long-poll listeners, the
/// test drives the flow deterministically: each round forwards every pending
/// outbox message onto a fresh bus, then drains the consumers (the saga
/// `subscribe`s to events, the worker services `listen` for commands). The loop
/// ends when no service has pending outbox work — i.e. the saga is complete.
///
/// ```text
/// StartSaga -> CreateOrder -> order.initialized -> ReserveInventory -> InventoryReserved
///   ─▶ ProcessPayment ─▶ PaymentSucceeded ─▶ CompleteOrder ─▶ OrderCompleted ─▶ done
/// ```
#[tokio::test]
async fn saga_distributed() {
    // === SAGA SERVICE ===
    let saga_repo = InMemoryRepository::new();
    let saga_outbox = saga_repo.outbox_store();
    let saga_svc = Arc::new(Service::new().named("saga").routes(distributed::routes!(
        Routes::new()
            .with_repo(saga_repo.clone().queued().aggregate::<OrderFulfillmentSaga>())
            .typed_command(handlers::saga::start::command())
            .handle(handlers::saga::start::handle),
        event handlers::saga::on_order_created,
        event handlers::saga::on_inventory_reserved,
        event handlers::saga::on_payment_succeeded,
        event handlers::saga::on_order_completed,
    )));

    // === ORDER SERVICE ===
    let order_repo = InMemoryRepository::new();
    let order_outbox = order_repo.outbox_store();
    let order_svc = Arc::new(
        Service::new().named("orders").routes(
            Routes::new()
                .with_repo(order_repo.clone().queued().aggregate::<Order>())
                .typed_command(handlers::orders::create::command())
                .handle(handlers::orders::create::handle)
                .typed_command(handlers::orders::complete::command())
                .handle(handlers::orders::complete::handle),
        ),
    );

    // === INVENTORY SERVICE (pre-seeded) ===
    let inventory_repo = InMemoryRepository::new();
    let inventory_outbox = inventory_repo.outbox_store();
    {
        let tmp = inventory_repo.clone().aggregate::<Inventory>();
        let mut inv = Inventory::new();
        inv.initialize("WIDGET-001".to_string(), 100).unwrap();
        tmp.commit(&mut inv).await.unwrap();
    }
    let inventory_svc = Arc::new(
        Service::new().named("inventory").routes(
            Routes::new()
                .with_repo(inventory_repo.clone().queued().aggregate::<Inventory>())
                .typed_command(handlers::inventory::init::command())
                .handle(handlers::inventory::init::handle)
                .typed_command(handlers::inventory::reserve::command())
                .handle(handlers::inventory::reserve::handle),
        ),
    );

    // === PAYMENT SERVICE ===
    let payment_repo = InMemoryRepository::new();
    let payment_outbox = payment_repo.outbox_store();
    let payment_svc = Arc::new(
        Service::new().named("payments").routes(
            Routes::new()
                .with_repo(payment_repo.clone().queued().aggregate::<Payment>())
                .typed_command(handlers::payments::process::command())
                .handle(handlers::payments::process::handle),
        ),
    );

    // === START THE SAGA ===
    dispatch(
        &saga_svc,
        "saga.start",
        json!({
            "saga_id": "saga-001",
            "order_id": "order-001",
            "customer_id": "customer-001",
            "items": [{ "sku": "WIDGET-001", "quantity": 5, "price_cents": 1000 }],
            "total_cents": 5000
        }),
        Session::new(),
    )
    .await
    .unwrap();

    // === DRIVE THE CHOREOGRAPHY OVER THE BUS UNTIL QUIESCENT ===
    let mut reached_quiescence = false;
    for _ in 0..30 {
        // A fresh bus per round bounds delivery to this hop's messages (the
        // in-memory topic log is retained across reads, so reusing one bus would
        // re-deliver every prior event to the saga).
        let bus = InMemoryBus::new();
        let published = publish_pending_outbox(&saga_outbox, &bus).await
            + publish_pending_outbox(&order_outbox, &bus).await
            + publish_pending_outbox(&inventory_outbox, &bus).await
            + publish_pending_outbox(&payment_outbox, &bus).await;
        if published == 0 {
            reached_quiescence = true;
            break;
        }

        bus.subscribe(saga_svc.clone(), RunOptions::until_idle())
            .await
            .expect("saga should drain its events");
        bus.listen(order_svc.clone(), RunOptions::until_idle())
            .await
            .expect("order service should drain its commands");
        bus.listen(inventory_svc.clone(), RunOptions::until_idle())
            .await
            .expect("inventory service should drain its commands");
        bus.listen(payment_svc.clone(), RunOptions::until_idle())
            .await
            .expect("payment service should drain its commands");
    }
    assert!(
        reached_quiescence,
        "the saga choreography should reach quiescence within the round budget"
    );

    // === VERIFY FINAL STATE — typed repos return aggregates directly ===

    let saga = saga_repo
        .queued()
        .aggregate::<OrderFulfillmentSaga>()
        .peek("saga-001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saga.status(), SagaStatus::Completed);
    assert!(saga.is_complete());

    let order = order_repo
        .queued()
        .aggregate::<Order>()
        .peek("order-001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(order.status(), OrderStatus::Completed);

    let inv = inventory_repo
        .queued()
        .aggregate::<Inventory>()
        .peek("WIDGET-001")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inv.available(), 95);
    assert_eq!(inv.reserved(), 5);

    let payment = payment_repo
        .queued()
        .aggregate::<Payment>()
        .peek("pay-order-001")
        .await
        .unwrap()
        .unwrap();
    assert!(payment.is_successful());
}
