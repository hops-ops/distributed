//! End-to-end durable-enqueue dispatch over a real SQL backend (in-memory
//! SQLite). Exercises `repo.outbox(msg).commit(agg)` (claim-in-transaction +
//! immediate publish, enabled by `with_bus`) and the `with_bus` runtime against a
//! persistent repository, not just the in-memory `InMemoryRepository` covered by
//! the unit tests.

use serde::{Deserialize, Serialize};
use serde_json::json;

use distributed::bus::{Bus, BusConsumer, Handlers, InMemoryBus, Message, MessageKind, RunOptions};
use distributed::command::{typed_command, PreparedCommand, Succeeded, TypedCommand};
use distributed::microsvc::{
    CausalCommandContext, HandlerError, HasOutboxStore, Routes, Service, Session, TRANSPORT_ROLE,
};
use distributed::{
    sourced, AggregateBuilder, AggregateRepository, Entity, OutboxMessage, OutboxMessageStatus,
    OutboxStore, Queueable, QueuedRepository, SqliteRepository,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Counter {
    entity: Entity,
    value: i64,
}

#[sourced(entity, aggregate_type = "counter")]
impl Counter {
    #[event("touched")]
    fn touch(&mut self, id: String) {
        self.entity.set_id(&id);
        self.value += 1;
    }
}

type Repo = AggregateRepository<QueuedRepository<SqliteRepository>, Counter>;

use common::commands;

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct Touch {}

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct Touched {
    value: i64,
}

/// Dispatched in-process with no role, and delivered by the bus.
fn touch() -> TypedCommand<Touch, Succeeded<Touched>> {
    typed_command::<Touch, Succeeded<Touched>>("counter.touch")
}

async fn handle_touch(
    ctx: &CausalCommandContext<'_, Counter>,
    _input: Touch,
) -> Result<PreparedCommand<Succeeded<Touched>>, HandlerError> {
    let mut counter = ctx.repo().create();
    counter.touch("c1".to_string())?;
    let message = OutboxMessage::create("evt-c1", "counter.touched", b"{}".to_vec())?;
    let value = counter.value;
    ctx.repo()
        .outbox(message)
        .commit(counter)?
        .succeeded(Touched { value })
}

async fn service() -> Repo {
    SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("sqlite repository should migrate")
        .queued()
        .aggregate::<Counter>()
}

async fn assert_published_and_drained(store: &impl OutboxStore, bus: &InMemoryBus) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            let mut remaining = 0;
            for status in [
                OutboxMessageStatus::Pending,
                OutboxMessageStatus::InFlight,
                OutboxMessageStatus::Failed,
                OutboxMessageStatus::Published,
            ] {
                remaining += store.messages_by_status(status, 8).await.unwrap().len();
            }
            if remaining == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("immediate publish should settle outbox rows");
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let record = delivered.clone();
    let handlers = Handlers::new().on_event("counter.touched", move |message: &Message| {
        record
            .lock()
            .unwrap()
            .push(message.id().unwrap().to_string());
        async { Ok(()) }
    });
    bus.subscribe(Arc::new(handlers), RunOptions::until_idle())
        .await
        .unwrap();
    assert_eq!(*delivered.lock().unwrap(), vec!["evt-c1".to_string()]);
}

#[tokio::test]
async fn commit_publishes_immediately_over_sqlite() {
    let bus = InMemoryBus::new();
    let repo = service().await;
    let store = repo.outbox_store();
    let service = Service::new()
        .named("counters")
        .routes(
            Routes::new()
                .with_repo(repo)
                .typed_command(touch())
                .handle(handle_touch),
        )
        .with_bus(bus.clone());

    // Command completion returns at durable commit; the bounded worker
    // claims the pending row and publishes it.
    commands::dispatch(&service, "counter.touch", json!({}), Session::new())
        .await
        .unwrap();

    assert_published_and_drained(&store, &bus).await;
    assert!(
        store.pending(usize::MAX).await.unwrap().is_empty(),
        "nothing should be left for the poller"
    );
}

#[tokio::test]
async fn run_consumes_command_and_publishes_over_sqlite() {
    let bus = InMemoryBus::new();
    let repo = service().await;
    let store = repo.outbox_store();
    let service = Service::new()
        .named("counters")
        .routes(
            Routes::new()
                .with_repo(repo)
                .typed_command(touch().roles([TRANSPORT_ROLE]))
                .handle(handle_touch),
        )
        .with_bus(bus.clone());

    // Enqueue a command, then run: listen is derived from the registered command,
    // drains it, and the handler publishes its outbox row through the bus.
    bus.send_message(
        Message::new("counter.touch", MessageKind::Command, b"{}".to_vec()).with_id("cmd-1"),
    )
    .await
    .unwrap();
    service.run(RunOptions::until_idle()).await.unwrap();

    assert_published_and_drained(&store, &bus).await;
}
