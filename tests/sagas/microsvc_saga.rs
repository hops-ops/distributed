//! Microservice Saga Tests
//!
//! Demonstrates the `routes!` convention with handler files
//! organized by service domain under `handlers/`.
//!
//! Each route bundle is typed to a specific aggregate via
//! `Routes::new().with_repo(repo.queued().aggregate::<T>())`, so handlers access
//! `ctx.repo().get()`, `ctx.repo().commit()`, etc. directly while `Service`
//! remains a non-generic deployment router.
//!
//! Two tests:
//! 1. **Orchestrated** — test runner dispatches commands to each service
//! 2. **Distributed** — services communicate over the async `InMemoryBus`

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use distributed::bus::{Bus, BusConsumer, InMemoryBus, RunOptions};
use distributed::microsvc::{Message, MessageKind, Routes, Service, Session};
use distributed::{
    AggregateBuilder, ClaimOutboxMessages, InMemoryOutboxStore, InMemoryRepository, OutboxClaimRef,
    OutboxStore, Queueable,
};

use super::handlers;
use super::order::{Inventory, Order, OrderFulfillmentSaga, OrderStatus, Payment, SagaStatus};

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

/// Test runner plays the role of the message bus, dispatching each command
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
    let saga_svc = Service::new().routes(distributed::routes!(
        Routes::new().with_repo(saga_repo.clone().queued().aggregate::<OrderFulfillmentSaga>()),
        command handlers::saga::start,
        event handlers::saga::on_order_created,
        event handlers::saga::on_inventory_reserved,
        event handlers::saga::on_payment_succeeded,
        event handlers::saga::on_order_completed,
    ));

    let order_repo = InMemoryRepository::new();
    let order_svc = Service::new().routes(distributed::routes!(
        Routes::new().with_repo(order_repo.clone().queued().aggregate::<Order>()),
        command handlers::orders::create,
        command handlers::orders::complete,
    ));

    let inventory_repo = InMemoryRepository::new();
    let inventory_svc = Service::new().routes(distributed::routes!(
        Routes::new().with_repo(inventory_repo.clone().queued().aggregate::<Inventory>()),
        command handlers::inventory::init,
        command handlers::inventory::reserve,
    ));

    let payment_repo = InMemoryRepository::new();
    let payment_svc = Service::new().routes(distributed::routes!(
        Routes::new().with_repo(payment_repo.clone().queued().aggregate::<Payment>()),
        command handlers::payments::process,
    ));

    let s = Session::new;

    // 1. Initialize inventory
    inventory_svc
        .dispatch(
            "inventory.initialize",
            json!({ "sku": "WIDGET-001", "stock": 100 }),
            s(),
        )
        .await
        .unwrap();

    // 2. Start saga → creates saga + outbox(CreateOrder)
    saga_svc
        .dispatch(
            "saga.start",
            json!({
                "saga_id": "saga-001",
                "order_id": "order-001",
                "customer_id": "customer-001",
                "items": [{ "sku": "WIDGET-001", "quantity": 5, "price_cents": 1000 }],
                "total_cents": 5000
            }),
            s(),
        )
        .await
        .unwrap();

    // 3. Create order -> outbox(order.initialized)
    order_svc
        .dispatch(
            "order.initialize",
            json!({
                "saga_id": "saga-001",
                "order_id": "order-001",
                "customer_id": "customer-001",
                "items": [{ "sku": "WIDGET-001", "quantity": 5, "price_cents": 1000 }],
                "total_cents": 5000
            }),
            s(),
        )
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
        .dispatch(
            "inventory.reserve",
            json!({
                "saga_id": "saga-001",
                "order_id": "order-001",
                "sku": "WIDGET-001",
                "quantity": 5
            }),
            s(),
        )
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
        .dispatch(
            "payment.process",
            json!({
                "saga_id": "saga-001",
                "order_id": "order-001",
                "amount_cents": 5000
            }),
            s(),
        )
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
        .dispatch(
            "order.complete",
            json!({ "saga_id": "saga-001", "order_id": "order-001" }),
            s(),
        )
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
    let saga_svc = Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_repo(saga_repo.clone().queued().aggregate::<OrderFulfillmentSaga>()),
        command handlers::saga::start,
        event handlers::saga::on_order_created,
        event handlers::saga::on_inventory_reserved,
        event handlers::saga::on_payment_succeeded,
        event handlers::saga::on_order_completed,
    )));

    // === ORDER SERVICE ===
    let order_repo = InMemoryRepository::new();
    let order_outbox = order_repo.outbox_store();
    let order_svc = Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_repo(order_repo.clone().queued().aggregate::<Order>()),
        command handlers::orders::create,
        command handlers::orders::complete,
    )));

    // === INVENTORY SERVICE (pre-seeded) ===
    let inventory_repo = InMemoryRepository::new();
    let inventory_outbox = inventory_repo.outbox_store();
    {
        let tmp = inventory_repo.clone().aggregate::<Inventory>();
        let mut inv = Inventory::new();
        inv.initialize("WIDGET-001".to_string(), 100).unwrap();
        tmp.commit(&mut inv).await.unwrap();
    }
    let inventory_svc = Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_repo(inventory_repo.clone().queued().aggregate::<Inventory>()),
        command handlers::inventory::init,
        command handlers::inventory::reserve,
    )));

    // === PAYMENT SERVICE ===
    let payment_repo = InMemoryRepository::new();
    let payment_outbox = payment_repo.outbox_store();
    let payment_svc = Arc::new(Service::new().routes(distributed::routes!(
        Routes::new().with_repo(payment_repo.clone().queued().aggregate::<Payment>()),
        command handlers::payments::process,
    )));

    // === START THE SAGA ===
    saga_svc
        .dispatch(
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
