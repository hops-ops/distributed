mod checkout;
mod checkout_saga_service;
use common::commands;
use common::env as env_support;
use common::ids;
#[cfg(feature = "postgres")]
use common::postgres;
mod projection_service;
mod query_service;
mod read_models;
mod seat_inventory_service;

use std::time::Duration;

use checkout::{
    checkout_command, seat_command, AddSeat, StartCheckout, CHECKOUT_SEAT_RESERVED, SEAT_RESERVED,
    SEAT_RESERVED_MESSAGE,
};
use checkout::{
    checkout_event, json_outbox_event, seat_event, CheckoutStarted, SeatAdded,
    SeatReservationCompleted, SeatReserved, CHECKOUT_STARTED, RESERVING_SEAT_MESSAGE,
    SEAT_AVAILABLE,
};
use checkout_saga_service::CheckoutSaga;
use distributed::microsvc::{Context, Routes, Service, Session};
use distributed::read_model::ReadModelWritePlanBuilder;
#[cfg(feature = "sqlite")]
use distributed::SqliteRepository;
use distributed::{
    AggregateBuilder, CommitBuilderExt, GetStream, OutboxMessage, OutboxStore,
    ReadModelWritePlanStore, RelationalReadModel, RelationalReadModelIncludes,
    RelationalReadModelQueryStore, TableStoreError, TransactionalCommit,
};
use distributed::{InMemoryReadModelStore, InMemoryRepository, Queueable};
use projection_service::service as projection_service;
use query_service::CheckoutQueryService;
use read_models::{register_schemas, CheckoutView};
use read_models::{CheckoutStepView, SeatView};
use seat_inventory_service::Seat;
use serde::Serialize;

async fn dispatch<C>(service: &Service, command: &str, input: C)
where
    C: Serialize,
{
    commands::dispatch(
        service,
        command,
        serde_json::to_value(input).expect("command should encode"),
        Session::new(),
    )
    .await
    .unwrap_or_else(|err| panic!("{command} should dispatch: {err:?}"));
}

struct FlowIds {
    checkout_id: String,
    seat_id: String,
    category: String,
}

use ids::unique_id;

#[allow(dead_code)]
async fn run_persistent_checkout_flow<R, CheckoutOutbox, SeatOutbox>(
    checkout_repo: R,
    checkout_outbox: CheckoutOutbox,
    seat_repo: R,
    seat_outbox: SeatOutbox,
    read_repo: R,
    ids: FlowIds,
) where
    R: Clone
        + GetStream
        + ReadModelWritePlanStore
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Send
        + Sync
        + 'static,
    CheckoutOutbox: OutboxStore + Send + Sync,
    SeatOutbox: OutboxStore + Send + Sync,
{
    let seat_added = add_seat(&seat_repo, &ids.seat_id, &ids.category).await;
    assert_pending(&seat_outbox, &seat_added).await;
    project_message(&read_repo, &seat_added).await;

    let checkout_started = start_checkout(
        &checkout_repo,
        &ids.checkout_id,
        &ids.seat_id,
        &ids.category,
    )
    .await;
    assert_pending(&checkout_outbox, &checkout_started).await;
    project_message(&read_repo, &checkout_started).await;

    let seat_reserved = reserve_started_checkout_seat(&seat_repo, &checkout_started).await;
    assert_pending(&seat_outbox, &seat_reserved).await;
    project_message(&read_repo, &seat_reserved).await;

    let reservation_completed = record_seat_reserved(&checkout_repo, &seat_reserved).await;
    assert_pending(&checkout_outbox, &reservation_completed).await;
    project_message(&read_repo, &reservation_completed).await;

    let checkout = load_checkout_screen(&read_repo, &ids.checkout_id)
        .await
        .expect("checkout read model load should succeed")
        .expect("checkout should be projected");
    assert_checkout_screen(&checkout, &ids);

    let seat = load_seat(&read_repo, &ids.seat_id)
        .await
        .expect("seat read model load should succeed")
        .expect("seat should be projected");
    assert_projected_seat(&seat, &ids);

    let loaded_checkout = checkout_repo
        .clone()
        .aggregate::<CheckoutSaga>()
        .get(&ids.checkout_id)
        .await
        .expect("checkout saga should reload")
        .expect("checkout saga should exist");
    assert_eq!(loaded_checkout.status, CHECKOUT_SEAT_RESERVED);
    assert_eq!(loaded_checkout.reserved_seat_id, ids.seat_id);

    let loaded_seat = seat_repo
        .clone()
        .aggregate::<Seat>()
        .get(&ids.seat_id)
        .await
        .expect("seat aggregate should reload")
        .expect("seat aggregate should exist");
    assert_eq!(loaded_seat.status, SEAT_RESERVED);
    assert_eq!(loaded_seat.checkout_id, ids.checkout_id);
}

async fn add_seat<R>(repo: &R, seat_id: &str, category: &str) -> OutboxMessage
where
    R: TransactionalCommit + Send + Sync,
{
    let mut seat = Seat::default();
    seat.add(seat_id.to_string(), category.to_string())
        .expect("seat should be valid");
    let event = SeatAdded {
        seat_id: seat_id.to_string(),
        category: category.to_string(),
    };
    let outbox = json_outbox_event(seat_id, seat_event::ADDED, &event)
        .expect("seat added outbox should encode");

    repo.outbox(outbox.clone())
        .commit(&mut seat)
        .await
        .expect("seat add should commit");
    outbox
}

async fn start_checkout<R>(
    repo: &R,
    checkout_id: &str,
    seat_id: &str,
    seat_category: &str,
) -> OutboxMessage
where
    R: TransactionalCommit + Send + Sync,
{
    let mut saga = CheckoutSaga::default();
    saga.start(
        checkout_id.to_string(),
        seat_id.to_string(),
        seat_category.to_string(),
    )
    .expect("checkout should be valid");
    let event = CheckoutStarted {
        checkout_id: checkout_id.to_string(),
        seat_id: seat_id.to_string(),
        seat_category: seat_category.to_string(),
    };
    let outbox = json_outbox_event(checkout_id, checkout_event::STARTED, &event)
        .expect("checkout started outbox should encode");

    repo.outbox(outbox.clone())
        .commit(&mut saga)
        .await
        .expect("checkout start should commit");
    outbox
}

async fn reserve_started_checkout_seat<R>(
    repo: &R,
    checkout_started: &OutboxMessage,
) -> OutboxMessage
where
    R: Clone + GetStream + TransactionalCommit + Send + Sync + 'static,
{
    let msg: CheckoutStarted = serde_json::from_slice(&checkout_started.payload)
        .expect("checkout started payload should decode");
    let mut seat = repo
        .clone()
        .aggregate::<Seat>()
        .get(&msg.seat_id)
        .await
        .expect("seat should load")
        .expect("seat should exist");
    assert_eq!(seat.status, SEAT_AVAILABLE);
    assert_eq!(seat.category, msg.seat_category);

    seat.reserve(msg.checkout_id.clone())
        .expect("seat should reserve");
    let event = SeatReserved {
        checkout_id: msg.checkout_id.clone(),
        seat_id: msg.seat_id.clone(),
        seat_category: msg.seat_category.clone(),
    };
    let outbox = json_outbox_event(&msg.checkout_id, seat_event::RESERVED, &event)
        .expect("seat reserved outbox should encode");

    repo.outbox(outbox.clone())
        .commit(&mut seat)
        .await
        .expect("seat reservation should commit");
    outbox
}

async fn record_seat_reserved<R>(repo: &R, seat_reserved: &OutboxMessage) -> OutboxMessage
where
    R: Clone + GetStream + TransactionalCommit + Send + Sync + 'static,
{
    let msg: SeatReserved = serde_json::from_slice(&seat_reserved.payload)
        .expect("seat reserved payload should decode");
    let mut saga = repo
        .clone()
        .aggregate::<CheckoutSaga>()
        .get(&msg.checkout_id)
        .await
        .expect("checkout saga should load")
        .expect("checkout saga should exist");
    assert_eq!(saga.status, CHECKOUT_STARTED);

    saga.set_reserved_seat(msg.seat_id.clone())
        .expect("checkout saga should record reserved seat");
    let event = SeatReservationCompleted {
        checkout_id: msg.checkout_id.clone(),
        seat_id: msg.seat_id.clone(),
        seat_category: msg.seat_category.clone(),
    };
    let outbox = json_outbox_event(
        &msg.checkout_id,
        checkout_event::SEAT_RESERVATION_COMPLETED,
        &event,
    )
    .expect("seat reservation completed outbox should encode");

    repo.outbox(outbox.clone())
        .commit(&mut saga)
        .await
        .expect("checkout saga update should commit");
    outbox
}

async fn project_message<R>(repo: &R, message: &OutboxMessage)
where
    R: ReadModelWritePlanStore + Send + Sync,
{
    let mut read_models = ReadModelWritePlanBuilder::new();

    match message.event_type.as_str() {
        checkout_event::STARTED => {
            let msg: CheckoutStarted =
                serde_json::from_slice(&message.payload).expect("checkout started should decode");
            let checkout = CheckoutView {
                checkout_id: msg.checkout_id.clone(),
                seat_id: msg.seat_id.clone(),
                seat_category: msg.seat_category,
                status: CHECKOUT_STARTED.to_string(),
                screen_message: RESERVING_SEAT_MESSAGE.to_string(),
                steps: Vec::new(),
                seat: None,
            };
            let step = CheckoutStepView {
                checkout_id: msg.checkout_id,
                step: "started".to_string(),
                detail: "checkout started".to_string(),
            };
            read_models
                .upsert(&checkout)
                .expect("checkout view should serialize")
                .upsert(&step)
                .expect("checkout step should serialize");
        }
        checkout_event::SEAT_RESERVATION_COMPLETED => {
            let msg: SeatReservationCompleted = serde_json::from_slice(&message.payload)
                .expect("seat reservation completed should decode");
            let checkout = CheckoutView {
                checkout_id: msg.checkout_id.clone(),
                seat_id: msg.seat_id,
                seat_category: msg.seat_category,
                status: CHECKOUT_SEAT_RESERVED.to_string(),
                screen_message: SEAT_RESERVED_MESSAGE.to_string(),
                steps: Vec::new(),
                seat: None,
            };
            let step = CheckoutStepView {
                checkout_id: msg.checkout_id,
                step: "seat_reservation_completed".to_string(),
                detail: "seat reservation completed".to_string(),
            };
            read_models
                .upsert(&checkout)
                .expect("checkout view should serialize")
                .upsert(&step)
                .expect("checkout step should serialize");
        }
        seat_event::ADDED => {
            let msg: SeatAdded =
                serde_json::from_slice(&message.payload).expect("seat added should decode");
            let seat = SeatView {
                seat_id: msg.seat_id,
                category: msg.category,
                status: SEAT_AVAILABLE.to_string(),
                checkout_id: String::new(),
            };
            read_models
                .upsert(&seat)
                .expect("seat view should serialize");
        }
        seat_event::RESERVED => {
            let msg: SeatReserved =
                serde_json::from_slice(&message.payload).expect("seat reserved should decode");
            let seat = SeatView {
                seat_id: msg.seat_id.clone(),
                category: msg.seat_category,
                status: SEAT_RESERVED.to_string(),
                checkout_id: msg.checkout_id.clone(),
            };
            let step = CheckoutStepView {
                checkout_id: msg.checkout_id,
                step: "seat_reserved".to_string(),
                detail: "seat reserved".to_string(),
            };
            read_models
                .upsert(&seat)
                .expect("seat view should serialize")
                .upsert(&step)
                .expect("checkout step should serialize");
        }
        other => panic!("unexpected projected event type {other}"),
    }

    read_models
        .commit(repo)
        .await
        .expect("projection read models should commit");
}

#[allow(dead_code)]
async fn assert_pending<S>(store: &S, message: &OutboxMessage)
where
    S: OutboxStore + Send + Sync,
{
    let pending = store
        .pending(usize::MAX)
        .await
        .expect("pending outbox messages should load");
    assert!(
        pending.iter().any(|pending| pending.id == message.id),
        "message {} should be pending in outbox",
        message.id
    );
}

async fn load_checkout_screen<R>(
    repo: &R,
    checkout_id: &str,
) -> Result<Option<CheckoutView>, TableStoreError>
where
    R: RelationalReadModelQueryStore + Send + Sync,
{
    let request = ReadModelWritePlanBuilder::new().load_with::<CheckoutView, _, _>(
        read_models::checkout_key(checkout_id),
        ["steps", "seat"],
    )?;
    let graph = repo.load_graph(request).await?;
    let Some(root) = graph.root else {
        return Ok(None);
    };

    let mut checkout = CheckoutView::from_row(root.data)?;
    for (include_name, include_rows) in graph.includes {
        let rows = include_rows
            .rows
            .into_iter()
            .map(|row| row.data)
            .collect::<Vec<_>>();
        checkout.hydrate_include(&include_name, rows)?;
    }
    Ok(Some(checkout))
}

async fn load_seat<R>(repo: &R, seat_id: &str) -> Result<Option<SeatView>, TableStoreError>
where
    R: RelationalReadModelQueryStore + Send + Sync,
{
    let request =
        ReadModelWritePlanBuilder::new().load::<SeatView>(read_models::seat_key(seat_id))?;
    let graph = repo.load_graph(request).await?;
    Ok(graph
        .root
        .map(|root| SeatView::from_row(root.data).expect("seat row should hydrate")))
}

fn assert_checkout_screen(checkout: &CheckoutView, ids: &FlowIds) {
    assert_eq!(checkout.seat_id, ids.seat_id);
    assert_eq!(checkout.seat_category, ids.category);
    assert_eq!(checkout.status, CHECKOUT_SEAT_RESERVED);
    assert_eq!(checkout.screen_message, SEAT_RESERVED_MESSAGE);
    assert_eq!(
        checkout
            .seat
            .as_ref()
            .expect("checkout should include seat")
            .status,
        SEAT_RESERVED
    );
    let mut steps: Vec<&str> = checkout
        .steps
        .iter()
        .map(|step| step.step.as_str())
        .collect();
    steps.sort();
    assert_eq!(
        steps,
        vec!["seat_reservation_completed", "seat_reserved", "started"]
    );
}

fn assert_projected_seat(seat: &SeatView, ids: &FlowIds) {
    assert_eq!(seat.status, SEAT_RESERVED);
    assert_eq!(seat.checkout_id, ids.checkout_id);
}

async fn publish_pending_outbox(
    outbox: &distributed::InMemoryOutboxStore,
    bus: &distributed::bus::InMemoryBus,
) {
    let claimed = outbox
        .claim(distributed::ClaimOutboxMessages::new(
            "matrix-outbox-bridge",
            64,
            Duration::from_secs(60),
        ))
        .await
        .expect("outbox claim should succeed");
    for message in claimed {
        let bus_message = Message::new(
            message.event_type.clone(),
            MessageKind::Event,
            message.payload.clone(),
        )
        .with_id(message.id().to_string());
        bus.publish_message(bus_message)
            .await
            .expect("outbox event should publish to the bus");
        let claim = distributed::OutboxClaimRef::from_message(&message)
            .expect("claimed message should yield a claim ref");
        outbox
            .complete(&claim)
            .await
            .expect("forwarded outbox message should complete");
    }
}

#[tokio::test]
async fn seat_checkout_saga_reserves_seat_and_projects_user_screen() {
    use distributed::bus::InMemoryBus;

    let ids = FlowIds {
        checkout_id: "checkout-1".to_string(),
        seat_id: "A-7".to_string(),
        category: "balcony".to_string(),
    };
    let checkout_store = InMemoryRepository::new();
    let checkout_service =
        checkout_saga_service::service(checkout_store.clone().queued().aggregate());
    let seat_store = InMemoryRepository::new();
    let seat_service = seat_inventory_service::service(seat_store.clone().queued().aggregate());
    let read_store = InMemoryReadModelStore::new();
    register_schemas(&read_store).expect("relational schemas should register");
    let projection_svc = projection_service(read_store.clone());
    let query_service = CheckoutQueryService::new(read_store.clone());

    let bus = InMemoryBus::new();

    dispatch(
        &seat_service,
        seat_command::ADD,
        AddSeat {
            seat_id: ids.seat_id.clone(),
            category: ids.category.clone(),
        },
    )
    .await;
    dispatch(
        &checkout_service,
        checkout_command::START,
        StartCheckout {
            checkout_id: ids.checkout_id.clone(),
            seat_id: ids.seat_id.clone(),
            seat_category: ids.category.clone(),
        },
    )
    .await;

    publish_pending_outbox(&seat_store.outbox_store(), &bus).await;
    publish_pending_outbox(&checkout_store.outbox_store(), &bus).await;
    bus.subscribe(projection_svc.clone(), RunOptions::until_idle())
        .await
        .expect("projection drains the opening events");
    bus.subscribe(seat_service.clone(), RunOptions::until_idle())
        .await
        .expect("seat service reacts to the started checkout");

    publish_pending_outbox(&seat_store.outbox_store(), &bus).await;
    bus.subscribe(projection_svc.clone(), RunOptions::until_idle())
        .await
        .expect("projection drains the reservation");
    bus.subscribe(checkout_service.clone(), RunOptions::until_idle())
        .await
        .expect("saga records the seat reservation");

    publish_pending_outbox(&checkout_store.outbox_store(), &bus).await;
    bus.subscribe(projection_svc.clone(), RunOptions::until_idle())
        .await
        .expect("projection drains the completion");

    let checkout = query_service
        .checkout_screen(&ids.checkout_id)
        .await
        .expect("checkout query should succeed")
        .expect("checkout should be projected");
    assert_checkout_screen(&checkout, &ids);

    let seat = query_service
        .seat(&ids.seat_id)
        .await
        .expect("seat query should succeed")
        .expect("seat should be projected");
    assert_projected_seat(&seat, &ids);

    let checkout_saga = checkout_store
        .clone()
        .queued()
        .aggregate::<CheckoutSaga>()
        .peek(&ids.checkout_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkout_saga.status, CHECKOUT_SEAT_RESERVED);
    assert_eq!(checkout_saga.reserved_seat_id, ids.seat_id);

    let seat = seat_store
        .clone()
        .queued()
        .aggregate::<Seat>()
        .peek(&ids.seat_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(seat.status, SEAT_RESERVED);
    assert_eq!(seat.checkout_id, ids.checkout_id);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_checkout_flow_projects_relational_read_models() {
    let checkout_repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("checkout repository should migrate");
    let checkout_outbox = checkout_repo.outbox_store();
    let seat_repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("seat repository should migrate");
    let seat_outbox = seat_repo.outbox_store();
    let read_repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("read repository should migrate");
    let registry = read_models::table_schema_registry().expect("schemas should build");
    read_repo
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .expect("read schema should bootstrap");

    run_persistent_checkout_flow(
        checkout_repo,
        checkout_outbox,
        seat_repo,
        seat_outbox,
        read_repo,
        FlowIds {
            checkout_id: unique_id("checkout-sqlite"),
            seat_id: unique_id("seat-sqlite"),
            category: "balcony".to_string(),
        },
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_checkout_flow_projects_relational_read_models() {
    let Some(schema) = postgres::PostgresTestSchema::create_from_env(
        "distributed_read",
        "Postgres distributed read-model test",
    )
    .await
    else {
        return;
    };

    let checkout_repo = schema.repository().await;
    let checkout_outbox = checkout_repo.outbox_store();
    let seat_repo = schema.repository().await;
    let seat_outbox = seat_repo.outbox_store();
    let read_repo = schema.repository().await;
    let registry = read_models::table_schema_registry().expect("schemas should build");
    read_repo
        .bootstrap_table_schema_for_dev(&registry)
        .await
        .expect("read schema should bootstrap");

    run_persistent_checkout_flow(
        checkout_repo,
        checkout_outbox,
        seat_repo,
        seat_outbox,
        read_repo,
        FlowIds {
            checkout_id: unique_id("checkout-postgres"),
            seat_id: unique_id("seat-postgres"),
            category: "balcony".to_string(),
        },
    )
    .await;
}

#[cfg(feature = "http")]
#[tokio::test]
async fn checkout_commands_can_be_http_service() {
    let checkout_store = InMemoryRepository::new();
    let checkout_service =
        checkout_saga_service::service(checkout_store.clone().queued().aggregate());
    let base = checkout_saga_service::start_http_service(checkout_service.clone()).await;

    let client = reqwest::Client::new();
    let started = client
        .post(format!("{base}/{}", checkout_command::START))
        // The identity a trusted proxy injects.
        .header("x-user-id", "user-1")
        .json(&serde_json::json!({
            "commandId": commands::command_id(),
            "input": StartCheckout {
                checkout_id: "checkout-http".to_string(),
                seat_id: "B-2".to_string(),
                seat_category: "floor".to_string(),
            },
        }))
        .send()
        .await
        .expect("HTTP checkout service should accept start request");
    assert_eq!(started.status(), 200);

    let saga = checkout_store
        .queued()
        .aggregate::<CheckoutSaga>()
        .peek("checkout-http")
        .await
        .expect("HTTP write-side load should succeed")
        .expect("HTTP write-side checkout should exist");
    assert_eq!(saga.status, checkout::CHECKOUT_STARTED);
}

#[cfg(feature = "grpc")]
#[tokio::test]
async fn checkout_commands_can_be_grpc_service() {
    let checkout_store = InMemoryRepository::new();
    let checkout_service =
        checkout_saga_service::service(checkout_store.clone().queued().aggregate());
    let mut client = checkout_saga_service::start_grpc_service(checkout_service.clone()).await;

    let started = client
        .dispatch(distributed::microsvc::grpc::GrpcRequest {
            command: checkout_command::START.to_string(),
            input: serde_json::json!({
                "commandId": commands::command_id(),
                "input": StartCheckout {
                    checkout_id: "checkout-grpc".to_string(),
                    seat_id: "C-4".to_string(),
                    seat_category: "box".to_string(),
                },
            })
            .to_string(),
            // The identity a trusted gateway supplies.
            session_variables: [("x-user-id".to_string(), "user-1".to_string())].into(),
        })
        .await
        .expect("gRPC checkout service should accept start request")
        .into_inner();
    assert_eq!(started.status, 200);

    let saga = checkout_store
        .queued()
        .aggregate::<CheckoutSaga>()
        .peek("checkout-grpc")
        .await
        .expect("gRPC write-side load should succeed")
        .expect("gRPC write-side checkout should exist");
    assert_eq!(saga.status, checkout::CHECKOUT_STARTED);
}

use std::collections::HashMap as StdHashMap;
use std::sync::{Arc as StdArc, Mutex as StdMutex};

use distributed::bus::{Bus, BusConsumer, RunOptions};
use distributed::microsvc::{Message, MessageKind};

const FLOW_EVENT_TYPES: [&str; 4] = [
    seat_event::ADDED,
    checkout_event::STARTED,
    seat_event::RESERVED,
    checkout_event::SEAT_RESERVATION_COMPLETED,
];

type Collected = StdArc<StdMutex<Vec<(String, String, Vec<u8>)>>>;

fn record_message(collected: &Collected, message: &Message) {
    collected.lock().unwrap().push((
        message.name().to_string(),
        message.id().unwrap_or_default().to_string(),
        message.payload().to_vec(),
    ));
}

fn build_collector() -> (StdArc<Service>, Collected) {
    let collected: Collected = StdArc::new(StdMutex::new(Vec::new()));
    let (c1, c2, c3, c4) = (
        collected.clone(),
        collected.clone(),
        collected.clone(),
        collected.clone(),
    );
    let service = Service::new().routes(
        Routes::new()
            .with_dependencies(())
            .event(seat_event::ADDED)
            .handle(move |ctx: &Context<()>| {
                record_message(&c1, ctx.message());
                async { Ok(serde_json::Value::Null) }
            })
            .event(checkout_event::STARTED)
            .handle(move |ctx: &Context<()>| {
                record_message(&c2, ctx.message());
                async { Ok(serde_json::Value::Null) }
            })
            .event(seat_event::RESERVED)
            .handle(move |ctx: &Context<()>| {
                record_message(&c3, ctx.message());
                async { Ok(serde_json::Value::Null) }
            })
            .event(checkout_event::SEAT_RESERVATION_COMPLETED)
            .handle(move |ctx: &Context<()>| {
                record_message(&c4, ctx.message());
                async { Ok(serde_json::Value::Null) }
            }),
    );
    (StdArc::new(service), collected)
}

async fn run_checkout_over_bus<B, R>(
    bus: B,
    collector: StdArc<Service>,
    collected: Collected,
    repo: R,
    ids: FlowIds,
) where
    B: Bus + BusConsumer,
    R: Clone
        + GetStream
        + ReadModelWritePlanStore
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Send
        + Sync
        + 'static,
{
    let seat_added = add_seat(&repo, &ids.seat_id, &ids.category).await;
    let checkout_started =
        start_checkout(&repo, &ids.checkout_id, &ids.seat_id, &ids.category).await;
    let seat_reserved = reserve_started_checkout_seat(&repo, &checkout_started).await;
    let reservation_completed = record_seat_reserved(&repo, &seat_reserved).await;
    let events = [
        seat_added,
        checkout_started,
        seat_reserved,
        reservation_completed,
    ];

    for event in &events {
        let message = Message::new(
            event.event_type.clone(),
            MessageKind::Event,
            event.payload.clone(),
        )
        .with_id(event.id().to_string());
        bus.publish_message(message)
            .await
            .expect("event should publish over the bus");
    }

    bus.subscribe(collector, RunOptions::until_idle())
        .await
        .expect("subscriber should drain the bus");

    project_and_assert_checkout(&repo, &ids, &delivered_map(&collected)).await;
}

fn delivered_map(collected: &Collected) -> StdHashMap<String, (String, Vec<u8>)> {
    collected
        .lock()
        .unwrap()
        .iter()
        .map(|(name, id, payload)| (name.clone(), (id.clone(), payload.clone())))
        .collect()
}

async fn project_and_assert_checkout<R>(
    repo: &R,
    ids: &FlowIds,
    delivered: &StdHashMap<String, (String, Vec<u8>)>,
) where
    R: ReadModelWritePlanStore + RelationalReadModelQueryStore + Send + Sync,
{
    for event_type in FLOW_EVENT_TYPES {
        let (id, payload) = delivered
            .get(event_type)
            .unwrap_or_else(|| panic!("event {event_type} should arrive over the transport"));
        let message = OutboxMessage::create(id.clone(), event_type, payload.clone())
            .expect("delivered event should rebuild");
        project_message(repo, &message).await;
    }

    let checkout = load_checkout_screen(repo, &ids.checkout_id)
        .await
        .expect("checkout read model load should succeed")
        .expect("checkout should be projected");
    assert_checkout_screen(&checkout, ids);

    let seat = load_seat(repo, &ids.seat_id)
        .await
        .expect("seat read model load should succeed")
        .expect("seat should be projected");
    assert_projected_seat(&seat, ids);
}

fn matrix_ids(tag: &str) -> FlowIds {
    FlowIds {
        checkout_id: unique_id(&format!("checkout-{tag}")),
        seat_id: unique_id(&format!("seat-{tag}")),
        category: "balcony".to_string(),
    }
}

async fn run_matrix_cell<B, R>(bus: B, repo: R, tag: &str)
where
    B: Bus + BusConsumer,
    R: Clone
        + GetStream
        + ReadModelWritePlanStore
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Send
        + Sync
        + 'static,
{
    let (collector, collected) = build_collector();
    run_checkout_over_bus(bus, collector, collected, repo, matrix_ids(tag)).await;
}

#[tokio::test]
async fn matrix_in_memory_persistence_over_in_memory_bus() {
    use distributed::bus::InMemoryBus;
    run_matrix_cell(InMemoryBus::new(), inmem_matrix_repo(), "inmem-inmem").await;
}

fn inmem_matrix_repo() -> InMemoryRepository {
    let repo = InMemoryRepository::new();
    register_schemas(repo.model_store()).expect("read-model schemas should register");
    repo
}

#[cfg(feature = "sqlite")]
async fn sqlite_matrix_repo() -> SqliteRepository {
    let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("sqlite matrix repo should migrate");
    let registry = read_models::table_schema_registry().expect("schemas should build");
    repo.bootstrap_table_schema_for_dev(&registry)
        .await
        .expect("read-model schema should bootstrap");
    repo
}

#[cfg(feature = "knative")]
async fn run_checkout_over_knative<R>(repo: R, ids: FlowIds)
where
    R: Clone
        + GetStream
        + ReadModelWritePlanStore
        + RelationalReadModelQueryStore
        + TransactionalCommit
        + Send
        + Sync
        + 'static,
{
    use distributed::bus::KnativeBus;
    use distributed::microsvc::cloud_events_router;

    let (collector, collected) = build_collector();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("knative ingress should bind");
    let addr = listener.local_addr().expect("ingress addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, cloud_events_router(collector))
            .await
            .expect("knative ingress should serve");
    });

    let bus = KnativeBus::new(format!("http://{addr}"), "", "matrix-source", "", "");

    let seat_added = add_seat(&repo, &ids.seat_id, &ids.category).await;
    let checkout_started =
        start_checkout(&repo, &ids.checkout_id, &ids.seat_id, &ids.category).await;
    let seat_reserved = reserve_started_checkout_seat(&repo, &checkout_started).await;
    let reservation_completed = record_seat_reserved(&repo, &seat_reserved).await;
    for event in [
        &seat_added,
        &checkout_started,
        &seat_reserved,
        &reservation_completed,
    ] {
        let message = Message::new(
            event.event_type.clone(),
            MessageKind::Event,
            event.payload.clone(),
        )
        .with_id(event.id().to_string());
        bus.publish_message(message)
            .await
            .expect("CloudEvent should POST to the Knative ingress");
    }

    project_and_assert_checkout(&repo, &ids, &delivered_map(&collected)).await;
    server.abort();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn matrix_sqlite_persistence_over_in_memory_bus() {
    use distributed::bus::InMemoryBus;
    run_matrix_cell(
        InMemoryBus::new(),
        sqlite_matrix_repo().await,
        "sqlite-inmem",
    )
    .await;
}

#[cfg(feature = "knative")]
#[tokio::test]
async fn matrix_in_memory_persistence_over_knative() {
    run_checkout_over_knative(inmem_matrix_repo(), matrix_ids("inmem-knative")).await;
}

#[cfg(all(feature = "knative", feature = "sqlite"))]
#[tokio::test]
async fn matrix_sqlite_persistence_over_knative() {
    run_checkout_over_knative(sqlite_matrix_repo().await, matrix_ids("sqlite-knative")).await;
}

#[cfg(feature = "nats")]
fn nats_url() -> Option<String> {
    env_support::require_service("NATS_URL", "NATS read-model matrix cell")
}

#[cfg(feature = "nats")]
async fn nats_matrix_bus(ns: &str) -> distributed::bus::NatsBus {
    let url = nats_url().expect("NATS_URL set");
    let bus = distributed::bus::NatsBus::connect(&url)
        .group("matrix")
        .namespace(ns)
        .await
        .expect("nats connect")
        .with_fetch_timeout(Duration::from_millis(800));
    bus.ensure_stream().await.expect("nats stream");
    bus
}

#[cfg(feature = "nats")]
#[tokio::test]
async fn matrix_in_memory_persistence_over_nats_bus() {
    if nats_url().is_none() {
        return;
    }
    let ns = unique_id("ns").to_lowercase();
    run_matrix_cell(
        nats_matrix_bus(&ns).await,
        inmem_matrix_repo(),
        "inmem-nats",
    )
    .await;
}

#[cfg(all(feature = "nats", feature = "sqlite"))]
#[tokio::test]
async fn matrix_sqlite_persistence_over_nats_bus() {
    if nats_url().is_none() {
        return;
    }
    let ns = unique_id("ns").to_lowercase();
    run_matrix_cell(
        nats_matrix_bus(&ns).await,
        sqlite_matrix_repo().await,
        "sqlite-nats",
    )
    .await;
}

#[cfg(feature = "rabbitmq")]
fn amqp_url() -> Option<String> {
    env_support::require_service("AMQP_URL", "RabbitMQ read-model matrix cell")
}

#[cfg(feature = "rabbitmq")]
async fn rabbit_matrix_bus(ns: &str, collector: &StdArc<Service>) -> distributed::bus::RabbitBus {
    let url = amqp_url().expect("AMQP_URL set");
    let bus = distributed::bus::RabbitBus::connect(&url)
        .group("matrix")
        .namespace(ns)
        .await
        .expect("rabbit connect");
    // Topic exchange drops events with no bound queue, so bind before publishing.
    bus.ensure_subscription(collector.as_ref())
        .await
        .expect("rabbit subscription bind");
    bus
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
async fn matrix_in_memory_persistence_over_rabbit_bus() {
    if amqp_url().is_none() {
        return;
    }
    let ns = unique_id("ns").to_lowercase();
    let (collector, collected) = build_collector();
    let bus = rabbit_matrix_bus(&ns, &collector).await;
    run_checkout_over_bus(
        bus,
        collector,
        collected,
        inmem_matrix_repo(),
        matrix_ids("inmem-rabbit"),
    )
    .await;
}

#[cfg(all(feature = "rabbitmq", feature = "sqlite"))]
#[tokio::test]
async fn matrix_sqlite_persistence_over_rabbit_bus() {
    if amqp_url().is_none() {
        return;
    }
    let ns = unique_id("ns").to_lowercase();
    let (collector, collected) = build_collector();
    let bus = rabbit_matrix_bus(&ns, &collector).await;
    run_checkout_over_bus(
        bus,
        collector,
        collected,
        sqlite_matrix_repo().await,
        matrix_ids("sqlite-rabbit"),
    )
    .await;
}

#[cfg(feature = "kafka")]
fn kafka_brokers() -> Option<String> {
    env_support::require_service("KAFKA_BROKERS", "Kafka read-model matrix cell")
}

#[cfg(feature = "kafka")]
async fn kafka_matrix_bus(ns: &str) -> distributed::bus::KafkaBus {
    let brokers = kafka_brokers().expect("KAFKA_BROKERS set");
    distributed::bus::KafkaBus::connect(&brokers)
        .group("matrix")
        .namespace(ns)
        .await
        .expect("kafka connect")
        .with_fetch_timeout(Duration::from_secs(10))
}

#[cfg(feature = "kafka")]
#[tokio::test]
async fn matrix_in_memory_persistence_over_kafka_bus() {
    if kafka_brokers().is_none() {
        return;
    }
    let ns = unique_id("ns");
    run_matrix_cell(
        kafka_matrix_bus(&ns).await,
        inmem_matrix_repo(),
        "inmem-kafka",
    )
    .await;
}

#[cfg(all(feature = "kafka", feature = "sqlite"))]
#[tokio::test]
async fn matrix_sqlite_persistence_over_kafka_bus() {
    if kafka_brokers().is_none() {
        return;
    }
    let ns = unique_id("ns");
    run_matrix_cell(
        kafka_matrix_bus(&ns).await,
        sqlite_matrix_repo().await,
        "sqlite-kafka",
    )
    .await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn matrix_in_memory_persistence_over_postgres_bus() {
    use distributed::bus::PostgresBus;
    let Some(schema) =
        postgres::PostgresTestSchema::create_from_env("matrix_pgbus", "Postgres-bus matrix cell")
            .await
    else {
        return;
    };
    let bus_pool = schema.repository().await.pool().clone();
    let bus = PostgresBus::new(bus_pool).group("matrix");
    bus.ensure_tables().await.expect("postgres bus tables");
    run_matrix_cell(bus, inmem_matrix_repo(), "inmem-pgbus").await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn matrix_postgres_persistence_over_in_memory_bus() {
    use distributed::bus::InMemoryBus;
    let Some(schema) = postgres::PostgresTestSchema::create_from_env(
        "matrix_pg",
        "Postgres-persistence matrix cell",
    )
    .await
    else {
        return;
    };
    let repo = schema.repository().await;
    let registry = read_models::table_schema_registry().expect("schemas should build");
    repo.bootstrap_table_schema_for_dev(&registry)
        .await
        .expect("read-model schema should bootstrap");
    run_matrix_cell(InMemoryBus::new(), repo, "pg-inmem").await;
}

#[cfg(feature = "postgres")]
async fn postgres_matrix_repo() -> Option<(
    postgres::PostgresTestSchema,
    distributed::PostgresRepository,
)> {
    let schema = postgres::PostgresTestSchema::create_from_env(
        "matrix_pg",
        "Postgres-persistence matrix cell",
    )
    .await?;
    let repo = schema.repository().await;
    let registry = read_models::table_schema_registry().expect("schemas should build");
    repo.bootstrap_table_schema_for_dev(&registry)
        .await
        .expect("read-model schema should bootstrap");
    Some((schema, repo))
}

#[cfg(feature = "postgres")]
async fn postgres_matrix_bus(
) -> Option<(postgres::PostgresTestSchema, distributed::bus::PostgresBus)> {
    use distributed::bus::PostgresBus;
    let schema =
        postgres::PostgresTestSchema::create_from_env("matrix_pgbus", "Postgres-bus matrix cell")
            .await?;
    let bus = PostgresBus::new(schema.repository().await.pool().clone()).group("matrix");
    bus.ensure_tables().await.expect("postgres bus tables");
    Some((schema, bus))
}

#[cfg(all(feature = "postgres", feature = "sqlite"))]
#[tokio::test]
async fn matrix_sqlite_persistence_over_postgres_bus() {
    let Some((_bus_schema, bus)) = postgres_matrix_bus().await else {
        return;
    };
    run_matrix_cell(bus, sqlite_matrix_repo().await, "sqlite-pgbus").await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn matrix_postgres_persistence_over_postgres_bus() {
    let (Some((_schema, repo)), Some((_bus_schema, bus))) =
        (postgres_matrix_repo().await, postgres_matrix_bus().await)
    else {
        return;
    };
    run_matrix_cell(bus, repo, "pg-pgbus").await;
}

#[cfg(all(feature = "postgres", feature = "nats"))]
#[tokio::test]
async fn matrix_postgres_persistence_over_nats_bus() {
    if nats_url().is_none() {
        return;
    }
    let Some((_schema, repo)) = postgres_matrix_repo().await else {
        return;
    };
    let ns = unique_id("ns").to_lowercase();
    run_matrix_cell(nats_matrix_bus(&ns).await, repo, "pg-nats").await;
}

#[cfg(all(feature = "postgres", feature = "rabbitmq"))]
#[tokio::test]
async fn matrix_postgres_persistence_over_rabbit_bus() {
    if amqp_url().is_none() {
        return;
    }
    let Some((_schema, repo)) = postgres_matrix_repo().await else {
        return;
    };
    let ns = unique_id("ns").to_lowercase();
    let (collector, collected) = build_collector();
    let bus = rabbit_matrix_bus(&ns, &collector).await;
    run_checkout_over_bus(bus, collector, collected, repo, matrix_ids("pg-rabbit")).await;
}

#[cfg(all(feature = "postgres", feature = "kafka"))]
#[tokio::test]
async fn matrix_postgres_persistence_over_kafka_bus() {
    if kafka_brokers().is_none() {
        return;
    }
    let Some((_schema, repo)) = postgres_matrix_repo().await else {
        return;
    };
    let ns = unique_id("ns");
    run_matrix_cell(kafka_matrix_bus(&ns).await, repo, "pg-kafka").await;
}

#[cfg(all(feature = "postgres", feature = "knative"))]
#[tokio::test]
async fn matrix_postgres_persistence_over_knative() {
    let Some((_schema, repo)) = postgres_matrix_repo().await else {
        return;
    };
    run_checkout_over_knative(repo, matrix_ids("pg-knative")).await;
}
