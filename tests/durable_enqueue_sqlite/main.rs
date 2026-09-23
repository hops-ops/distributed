//! End-to-end durable-enqueue dispatch over a real SQL backend (in-memory
//! SQLite). Exercises `repo.outbox(msg).commit(agg)` (claim-in-transaction +
//! immediate publish, enabled by `with_bus`) and the `with_bus` runtime against a
//! persistent repository, not just the in-memory `InMemoryRepository` covered by
//! the unit tests.

#![cfg(feature = "sqlite")]

use serde_json::{json, Value};

use distributed::bus::{Bus, BusConsumer, Handlers, InMemoryBus, Message, RunOptions};
use distributed::microsvc::{Context, HandlerError, HasOutboxStore, Routes, Service, Session};
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

// Named fn (not a closure) so the higher-ranked `Handler` bound resolves.
async fn handle_touch(ctx: &Context<'_, Repo>) -> Result<Value, HandlerError> {
    let mut counter = Counter::default();
    counter.touch("c1".to_string())?;
    let message = OutboxMessage::create("evt-c1", "counter.touched", b"{}".to_vec())?;
    ctx.repo().outbox(message).commit(&mut counter).await?;
    Ok(json!({ "value": counter.value }))
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
    bus.subscribe(Arc::new(handlers), RunOptions::idempotent())
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
        .routes(
            Routes::new()
                .with_repo(repo)
                .command("counter.touch")
                .handle(handle_touch),
        )
        .with_bus(bus.clone());

    // Command completion returns at durable commit; the bounded worker
    // claims the pending row and publishes it.
    service
        .dispatch("counter.touch", json!({}), Session::new())
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
        .routes(
            Routes::new()
                .with_repo(repo)
                .command("counter.touch")
                .handle(handle_touch),
        )
        .with_bus(bus.clone());

    // Enqueue a command, then run: listen is derived from the registered command,
    // drains it, and the handler publishes its outbox row through the bus.
    bus.send("counter.touch", b"{}".to_vec()).await.unwrap();
    service.run(RunOptions::idempotent()).await.unwrap();

    assert_published_and_drained(&store, &bus).await;
}
