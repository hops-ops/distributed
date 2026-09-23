//! Bus integration for [`Service`]: `with_bus` (a builder step) and `run`.
//!
//! Attaching a bus does not change the service's type. `with_bus` is a plain
//! builder step on [`Service`] that (1) installs an outbox publisher on the
//! repository, so `repo.outbox(msg).commit(agg)` publishes immediately, and
//! (2) captures the bus's consume behavior as a type-erased closure on the
//! service. `run` drives that closure. There is no separate runtime type.

use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use super::service::DynBusPublisher;
use super::Service;
use crate::bus::{Bus, BusConsumer, RunOptions, TransportError};

/// Default lease for an immediate after-commit outbox publish. Short by design:
/// it only needs to cover commit → publish, so a crash before the publish
/// completes hands the row back to a separately operated polling worker
/// quickly.
pub const DEFAULT_PUBLISH_LEASE: Duration = Duration::from_secs(5);

/// Default publish-failure ceiling before an outbox row is permanently failed.
pub const DEFAULT_MAX_PUBLISH_ATTEMPTS: u32 = 5;

impl Service {
    /// Attach a bus — a builder step that returns the same [`Service`].
    ///
    /// Two effects, both composing with the rest of the builder:
    /// - installs an outbox publisher on the repository, so
    ///   `repo.outbox(msg).commit(agg)` writes pending rows and enqueues their
    ///   ids on a bounded worker after commit (overflow wakes `dispatch_batch`);
    /// - captures how to consume, so [`run`](Self::run) listens for the
    ///   registered command names (competing) and subscribes to the event names
    ///   (fan-out).
    ///
    /// The worker also polls pending rows, so a crash between commit and
    /// publish is recovered in-process. Spawn a separate
    /// [`crate::OutboxDrainRunner`] next to `run` only when you want an extra
    /// competing drainer.
    pub fn with_bus<B>(mut self, bus: B) -> Self
    where
        B: Bus + BusConsumer + 'static,
    {
        let bus = Arc::new(bus);

        self.configure_outbox_publishers(
            DynBusPublisher::new(Arc::clone(&bus)),
            format!("microsvc-immediate:{}", std::process::id()),
            DEFAULT_PUBLISH_LEASE,
            DEFAULT_MAX_PUBLISH_ATTEMPTS,
        );

        self.set_runner(Box::new(
            move |service: Arc<Service>, options: RunOptions| {
                let bus = Arc::clone(&bus);
                Box::pin(async move { run_consumers(&*bus, service, options).await })
            },
        ));
        self
    }

    /// Run against the bus attached with [`with_bus`](Self::with_bus): consume
    /// the registered command names (competing `listen`) and event names
    /// (fan-out `subscribe`) concurrently on the caller's runtime. Returns when
    /// the consumers stop (a pull source that drains, or the first error).
    ///
    /// Producing is handled on the commit path — `repo.outbox(msg).commit(agg)`
    /// publishes immediately once a bus is attached.
    ///
    /// # Errors
    /// Returns a permanent [`TransportError`] if no bus was attached — call
    /// [`with_bus`](Self::with_bus) first.
    pub async fn run(mut self, options: RunOptions) -> Result<(), TransportError> {
        let Some(runner) = self.take_runner() else {
            return Err(TransportError::permanent(
                "Service::run requires a bus; call `with_bus` first",
            ));
        };
        self.bootstrap_projectors()
            .await
            .map_err(TransportError::from)?;
        runner(Arc::new(self), options).await
    }

    /// Build a drain runner over `store` that publishes through `publisher`.
    ///
    /// Worker id is `drain:<pid>` so claims never collide with the immediate
    /// hook's `microsvc-immediate:<pid>`. Spawn it next to [`run`](Self::run);
    /// this does not start a task.
    #[cfg(any(
        feature = "http",
        feature = "grpc",
        feature = "postgres",
        feature = "sqlite",
        feature = "nats",
        feature = "rabbitmq",
        feature = "kafka",
        test,
    ))]
    pub fn outbox_drain<S, P>(
        store: S,
        publisher: P,
        max_attempts: u32,
    ) -> crate::OutboxDrainRunner<S, P>
    where
        S: crate::OutboxStore,
        P: crate::bus::MessagePublisher,
    {
        crate::OutboxDrainRunner::for_store(store, publisher, max_attempts)
    }
}

/// A running transport consumer (a `listen` or `subscribe` loop), borrowing the
/// bus for `'b`.
type ConsumerFuture<'b> = Pin<Box<dyn Future<Output = Result<(), TransportError>> + Send + 'b>>;

/// Drive a service's command/event consumers concurrently on the caller's
/// runtime — no spawn, no timer. Returns on the first error; finishes when all
/// consumers stop.
async fn run_consumers<'b, B>(
    bus: &'b B,
    service: Arc<Service>,
    options: RunOptions,
) -> Result<(), TransportError>
where
    B: Bus + BusConsumer,
{
    let plan = service.subscription_plan();
    let mut consumers: Vec<ConsumerFuture<'b>> = Vec::new();
    if !plan.commands.is_empty() {
        consumers.push(Box::pin(bus.listen(Arc::clone(&service), options.clone())));
    }
    if !plan.events.is_empty() {
        consumers.push(Box::pin(bus.subscribe(Arc::clone(&service), options)));
    }

    poll_fn(move |cx| {
        let mut index = 0;
        while index < consumers.len() {
            match consumers[index].as_mut().poll(cx) {
                Poll::Ready(Ok(())) => {
                    // Drop the finished consumer future; nothing left to poll.
                    let _finished = consumers.remove(index);
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => index += 1,
            }
        }
        if consumers.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::bus::{Bus, InMemoryBus, RunOptions};
    use crate::microsvc::{Context, HandlerError, Routes, Service, Session};
    use crate::outbox_worker::OutboxStore;

    async fn assert_published_and_drained(store: &impl OutboxStore, bus: &InMemoryBus, id: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                // Delivery evidence is in the bus, not in deleted outbox rows.
                let delivered = bus.published_ids().iter().any(|actual| actual == id);
                let mut remaining = 0;
                for status in [
                    OutboxMessageStatus::Pending,
                    OutboxMessageStatus::InFlight,
                    OutboxMessageStatus::Failed,
                    OutboxMessageStatus::Published,
                ] {
                    remaining += store.messages_by_status(status, 8).await.unwrap().len();
                }
                if delivered && remaining == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bus must receive the event and outbox must remove its row");
        assert_eq!(bus.published_ids(), vec![id.to_string()]);
    }
    use crate::{
        sourced, AggregateBuilder, AggregateRepository, Entity, InMemoryRepository, OutboxMessage,
        OutboxMessageStatus, Queueable, QueuedRepository, Snapshot, TransactionalCommit,
    };

    #[derive(Default)]
    struct Dummy {
        entity: Entity,
    }

    #[sourced(entity)]
    impl Dummy {
        #[event("touched")]
        fn touch(&mut self) {
            if self.entity.id().is_empty() {
                self.entity.set_id("dummy-1");
            }
        }
    }

    #[tokio::test]
    async fn run_without_a_bus_is_a_permanent_error_not_a_panic() {
        let err = Service::new()
            .run(RunOptions::idempotent())
            .await
            .unwrap_err();

        assert!(err.is_permanent());
        assert!(
            err.message().contains("with_bus"),
            "error should point at the missing builder step, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn with_bus_configures_outbox_for_all_eligible_route_bundles() {
        let bus = InMemoryBus::new();
        let repo_a = InMemoryRepository::new();
        let store_a = repo_a.outbox_store();
        let repo_b = InMemoryRepository::new();
        let store_b = repo_b.outbox_store();
        let service = Service::new()
            .routes(
                Routes::new()
                    .with_repo(repo_a.queued().aggregate::<Dummy>())
                    .command("dummy.touch.a")
                    .handle(touch_and_publish),
            )
            .routes(
                Routes::new()
                    .with_repo(repo_b.queued().aggregate::<Dummy>())
                    .command("dummy.touch.b")
                    .handle(touch_and_publish),
            )
            .with_bus(bus.clone());

        service
            .dispatch("dummy.touch.a", json!({}), Session::new())
            .await
            .unwrap();
        service
            .dispatch("dummy.touch.b", json!({}), Session::new())
            .await
            .unwrap();

        assert_published_and_drained(&store_a, &bus, "evt-1").await;
        assert_published_and_drained(&store_b, &bus, "evt-1").await;
    }

    #[tokio::test]
    async fn with_bus_keeps_non_outbox_route_bundles_runnable() {
        let service = Service::new()
            .routes(
                Routes::new()
                    .with_dependencies(String::from("pong"))
                    .command("ping")
                    .handle(|ctx: &Context<String>| {
                        let reply = ctx.dependencies().clone();
                        async move { Ok(json!({ "reply": reply })) }
                    }),
            )
            .with_bus(InMemoryBus::new());

        let result = service
            .dispatch("ping", json!({}), Session::new())
            .await
            .unwrap();

        assert_eq!(result, json!({ "reply": "pong" }));
    }

    type TouchRepo = AggregateRepository<QueuedRepository<InMemoryRepository>, Dummy>;

    // A named fn (not a closure) so the higher-ranked `Handler` bound resolves.
    async fn touch_and_publish(ctx: &Context<'_, TouchRepo>) -> Result<Value, HandlerError> {
        let mut dummy = Dummy::default();
        dummy.touch()?;
        let message = OutboxMessage::create("evt-1", "dummy.touched", b"{}".to_vec())?;
        // The good old API — commit publishes immediately because a bus is attached.
        ctx.repo().outbox(message).commit(&mut dummy).await?;
        Ok(json!({ "ok": true }))
    }

    #[tokio::test]
    async fn dispatch_through_a_handler_publishes_immediately() {
        let bus = InMemoryBus::new();
        let repo = InMemoryRepository::new();
        let store = repo.outbox_store();
        let service = Service::new()
            .routes(
                Routes::new()
                    .with_repo(repo.queued().aggregate::<Dummy>())
                    .command("dummy.touch")
                    .handle(touch_and_publish),
            )
            .with_bus(bus.clone());

        // The handler runs `outbox().commit()`: pending row, then the
        // bounded worker publishes through the attached bus.
        service
            .dispatch("dummy.touch", json!({}), Session::new())
            .await
            .unwrap();

        assert_published_and_drained(&store, &bus, "evt-1").await;
    }

    #[tokio::test]
    async fn run_consumes_registered_commands_from_the_bus() {
        let bus = InMemoryBus::new();
        let repo = InMemoryRepository::new();
        let store = repo.outbox_store();
        let service = Service::new()
            .routes(
                Routes::new()
                    .with_repo(repo.queued().aggregate::<Dummy>())
                    .command("dummy.touch")
                    .handle(touch_and_publish),
            )
            .with_bus(bus.clone());

        // Enqueue a command on the bus, then run: `listen` is derived from the
        // registered command, drains the message, and the handler publishes.
        // `run` returns once the queue is empty (InMemoryBus yields `None`).
        bus.send("dummy.touch", b"{}".to_vec()).await.unwrap();
        service.run(RunOptions::idempotent()).await.unwrap();
        assert_published_and_drained(&store, &bus, "evt-1").await;
    }

    #[tokio::test]
    async fn outbox_drain_sugar_publishes_rows_the_hook_never_saw() {
        let repo = InMemoryRepository::new();
        let store = repo.outbox_store();
        let mut batch = crate::CommitBatch::empty();
        batch
            .outbox_messages
            .push(OutboxMessage::create("evt-left", "dummy.touched", b"{}".to_vec()).unwrap());
        repo.commit_batch(batch).await.unwrap();

        let bus = InMemoryBus::new();
        let handle = Service::outbox_drain(
            store.clone(),
            crate::BusPublisher::new(std::sync::Arc::new(bus.clone())),
            5,
        )
        .with_poll_interval(std::time::Duration::from_millis(5))
        .spawn();
        assert_published_and_drained(&store, &bus, "evt-left").await;
        handle.stop().await.unwrap();
    }

    #[derive(Default, Snapshot)]
    struct SnapCounter {
        entity: Entity,
        value: i64,
    }

    #[sourced(entity, aggregate_type = "snap_counter")]
    impl SnapCounter {
        #[event("touched")]
        fn touch(&mut self, id: String) {
            self.entity.set_id(&id);
            self.value += 1;
        }
    }

    // Snapshots are a transparent optimization: the repo type is unchanged.
    type SnapRepo = AggregateRepository<QueuedRepository<InMemoryRepository>, SnapCounter>;

    async fn touch_snap(ctx: &Context<'_, SnapRepo>) -> Result<Value, HandlerError> {
        let mut counter = SnapCounter::default();
        counter.touch("s1".to_string())?;
        let message = OutboxMessage::create("evt-s1", "snap.touched", b"{}".to_vec())?;
        ctx.repo().outbox(message).commit(&mut counter).await?;
        Ok(json!({}))
    }

    #[tokio::test]
    async fn outbox_commit_publishes_with_snapshot_backed_repo() {
        let bus = InMemoryBus::new();
        // `outbox().commit()` must work for a snapshot-backed repository too: the
        // outbox row and the snapshot commit together in one transaction, then
        // the row publishes immediately.
        let repo = InMemoryRepository::new();
        let store = repo.outbox_store();
        let service = Service::new()
            .routes(
                Routes::new()
                    .with_repo(repo.queued().aggregate::<SnapCounter>().with_snapshots(1))
                    .command("snap.touch")
                    .handle(touch_snap),
            )
            .with_bus(bus.clone());

        service
            .dispatch("snap.touch", json!({}), Session::new())
            .await
            .unwrap();
        assert_published_and_drained(&store, &bus, "evt-s1").await;
    }
}
