//! Typed commands delivered by a transport (a bus consumer, the Knative
//! ingress) run through the command ledger as the transport identity.
//!
//! - a command opts in by declaring [`TRANSPORT_ROLE`]; any other typed
//!   command is refused permanently;
//! - the ledger command id derives from the message id, so a redelivery
//!   replays the receipt and the handler does not run again;
//! - identity and role keys a message carries are never trusted.
//!
//! The scenarios run on the in-memory repository and, with `sqlite`, on a
//! SQLite file.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::json;

use distributed::bus::{
    Bus, InMemoryBus, InMemoryDeadLetters, Message, MessageKind, MessageRouter, RunOptions,
    TransportError,
};
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::microsvc::{
    CausalCommandContext, HandlerError, PortableCommand, Routes, Service,
    TRANSPORT_MESSAGE_ID_CLAIM, TRANSPORT_ROLE, TRANSPORT_USER_ID,
};
use distributed::{sourced, AggregateRepository, Entity, GetStream, StreamIdentity};

#[derive(Clone, Default, Serialize, Deserialize, distributed::Snapshot)]
struct Counter {
    entity: Entity,
    count: u32,
}

#[sourced(entity, aggregate_type = "Counter", events = "CounterEvent")]
impl Counter {
    #[event("counter.bumped", version = 1)]
    fn bump(&mut self, id: String) {
        self.entity.set_id(id);
        self.count += 1;
    }
}

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct BumpInput {
    id: String,
    /// `ok`, `reject` (a business refusal) or `flaky` (fails once per id
    /// with a server error, then succeeds).
    mode: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct BumpPayload {
    count: u32,
    user: String,
    roles: String,
    message_id: String,
    /// A claim the publisher set that is not an identity key.
    tenant: String,
    /// A host-only claim the publisher tried to set.
    schedule: String,
}

/// Declares the transport role: deliverable by a bus.
const BUMP: &str = "counter.bump";
/// Declares only `admin`.
const ADMIN_BUMP: &str = "counter.admin_bump";
/// Declares no role at all (any session is granted it).
const OPEN_BUMP: &str = "counter.open_bump";

/// Handler runs per counter id (each test of each backend uses its own).
static RUNS: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);
/// Counter ids whose next `flaky` run fails.
static FLAKY: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);

fn runs(id: &str) -> usize {
    RUNS.lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .get(id)
        .copied()
        .unwrap_or(0)
}

async fn handle_bump(
    ctx: &CausalCommandContext<'_, Counter>,
    input: BumpInput,
) -> Result<PreparedCommand<Succeeded<BumpPayload>>, HandlerError> {
    *RUNS
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(input.id.clone())
        .or_default() += 1;
    match input.mode.as_str() {
        "reject" => return Err(HandlerError::Rejected("not today".into())),
        "flaky" => {
            let mut flaky = FLAKY.lock().unwrap();
            let failures = flaky
                .get_or_insert_with(HashMap::new)
                .entry(input.id.clone())
                .or_default();
            if *failures == 0 {
                *failures += 1;
                return Err(HandlerError::Other("transient dependency outage".into()));
            }
        }
        _ => {}
    }
    let repo = ctx.repo();
    let mut counter = match repo.get(&input.id).await? {
        Some(counter) => counter,
        None => repo.create(),
    };
    counter
        .bump(input.id)
        .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    let payload = BumpPayload {
        count: counter.count,
        user: ctx.user_id()?.to_string(),
        roles: ctx.session().roles().join(","),
        message_id: ctx
            .claim(TRANSPORT_MESSAGE_ID_CLAIM)
            .unwrap_or_default()
            .to_string(),
        tenant: ctx.claim("x-tenant").unwrap_or_default().to_string(),
        schedule: ctx
            .claim("x-distributed-schedule")
            .unwrap_or_default()
            .to_string(),
    };
    repo.commit(counter)?.succeeded(payload)
}

struct Bump(&'static str, &'static [&'static str]);

impl<D> PortableCommand<D> for Bump
where
    D: distributed::microsvc::CausalRouteDependencies<Aggregate = Counter> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(
                typed_command::<BumpInput, Succeeded<BumpPayload>>(self.0).roles(self.1.to_vec()),
            )
            .handle(handle_bump)
    }
}

fn service<R>(repo: R) -> Service
where
    AggregateRepository<R, Counter>: distributed::microsvc::HasRepo
        + distributed::microsvc::HasOutboxStore
        + distributed::microsvc::ConfigurableOutboxPublisher
        + distributed::microsvc::CausalRouteDependencies<Aggregate = Counter>
        + Send
        + Sync
        + 'static,
{
    Service::new().named("counters").routes(
        Routes::new()
            .with_repo(AggregateRepository::<R, Counter>::new(repo))
            .mount(Bump(BUMP, &[TRANSPORT_ROLE]))
            .mount(Bump(ADMIN_BUMP, &["admin"]))
            .mount(Bump(OPEN_BUMP, &[])),
    )
}

fn command(name: &str, message_id: &str, counter: &str, mode: &str) -> Message {
    Message::new(
        name,
        MessageKind::Command,
        serde_json::to_vec(&json!({ "id": counter, "mode": mode })).unwrap(),
    )
    .with_id(message_id)
}

async fn bumps<R: GetStream>(repo: &R, counter: &str) -> usize {
    let identity = StreamIdentity::new("Counter", counter).unwrap();
    repo.get_stream(&identity)
        .await
        .unwrap()
        .map_or(0, |entity| entity.events().len())
}

/// Deliver as a bus consumer does: through the service's [`MessageRouter`].
async fn deliver(service: &Service, message: &Message) -> Result<(), TransportError> {
    MessageRouter::dispatch(service, message).await
}

macro_rules! scenarios {
    ($backend:literal, $repo_ty:ty, $fresh:expr) => {
        #[tokio::test]
        async fn a_delivered_command_commits_once_and_its_redelivery_replays() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let id = concat!($backend, "-once");

            let message = command(BUMP, "msg-1", id, "ok");
            let first = service.dispatch_message(&message).await.unwrap();
            let second = service.dispatch_message(&message).await.unwrap();
            assert_eq!(first, second, "a redelivery returns the stored receipt");
            assert_eq!(first["count"], 1);
            assert_eq!(first["user"], TRANSPORT_USER_ID);
            assert_eq!(first["roles"], TRANSPORT_ROLE);
            assert_eq!(first["message_id"], "msg-1");
            assert_eq!(runs(id), 1, "the handler ran once");
            assert_eq!(bumps(&repo, id).await, 1);

            // Another message is another command.
            let next = command(BUMP, "msg-2", id, "ok");
            assert_eq!(service.dispatch_message(&next).await.unwrap()["count"], 2);
            assert_eq!(bumps(&repo, id).await, 2);

            // One message id on another command name is another command too.
            let error = deliver(&service, &command(ADMIN_BUMP, "msg-1", id, "ok"))
                .await
                .unwrap_err();
            assert!(error.is_permanent(), "{error}");
        }

        #[tokio::test]
        async fn a_command_not_declaring_the_transport_role_is_refused_permanently() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            for (name, id) in [(ADMIN_BUMP, concat!($backend, "-undeclared-admin")), (OPEN_BUMP, concat!($backend, "-undeclared-open"))] {
                let message = command(name, "msg-1", id, "ok");
                for _delivery in 0..2 {
                    let error = deliver(&service, &message).await.unwrap_err();
                    assert!(error.is_permanent(), "{name}: {error}");
                }
                assert_eq!(runs(id), 0, "{name}");
                assert_eq!(bumps(&repo, id).await, 0, "{name}");
            }
        }

        #[tokio::test]
        async fn identity_a_message_carries_is_never_trusted() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let forge = |message: Message| {
                message
                    .with_metadata("x-user-id", "admin")
                    .with_metadata("X-User-Id", "admin")
                    .with_metadata("x-roles", "admin")
                    .with_metadata("X-Roles", "admin,transport")
                    .with_metadata("X-ROLES", "admin")
                    .with_metadata("x-distributed-message-id", "someone-elses-message")
                    .with_metadata("X-Distributed-Schedule", "tick")
                    .with_metadata("X-Tenant", "acme")
            };

            // The forged admin cannot run an admin-only command.
            let id = concat!($backend, "-forged-admin");
            let error = deliver(&service, &forge(command(ADMIN_BUMP, "msg-1", id, "ok")))
                .await
                .unwrap_err();
            assert!(error.is_permanent(), "{error}");
            assert_eq!(runs(id), 0);
            assert_eq!(bumps(&repo, id).await, 0);

            // On a transport command the same headers change nothing: the
            // handler sees the transport identity. Other metadata arrives.
            let id = concat!($backend, "-forged-bump");
            let payload = service
                .dispatch_message(&forge(command(BUMP, "msg-2", id, "ok")))
                .await
                .unwrap();
            assert_eq!(payload["user"], TRANSPORT_USER_ID);
            assert_eq!(payload["roles"], TRANSPORT_ROLE);
            assert_eq!(payload["message_id"], "msg-2");
            assert_eq!(payload["schedule"], "");
            assert_eq!(payload["tenant"], "acme");

            // The forged headers are no part of the command's identity: the
            // same message without them is the same command.
            let replay = service
                .dispatch_message(&command(BUMP, "msg-2", id, "ok"))
                .await
                .unwrap();
            assert_eq!(replay, payload);
            assert_eq!(runs(id), 1);
        }

        #[tokio::test]
        async fn a_command_message_without_an_id_is_refused_permanently() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let id = concat!($backend, "-no-id");
            let mut message = command(BUMP, "ignored", id, "ok");
            for missing in [None, Some(String::new())] {
                message.id = missing;
                let error = deliver(&service, &message).await.unwrap_err();
                assert!(error.is_permanent(), "{error}");
                assert!(error.message().contains("no id"), "{error}");
            }
            assert_eq!(runs(id), 0);
            assert_eq!(bumps(&repo, id).await, 0);
        }

        #[tokio::test]
        async fn a_rejection_is_permanent_and_replays_without_running_again() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let id = concat!($backend, "-rejected");
            let message = command(BUMP, "msg-1", id, "reject");
            for _delivery in 0..2 {
                let error = deliver(&service, &message).await.unwrap_err();
                assert!(error.is_permanent(), "{error}");
                assert!(error.message().contains("not today"), "{error}");
            }
            assert_eq!(runs(id), 1, "the stored rejection answers the redelivery");

            // The message id is spent: other input under it is refused.
            let error = deliver(&service, &command(BUMP, "msg-1", id, "ok"))
                .await
                .unwrap_err();
            assert!(error.is_permanent(), "{error}");
            assert_eq!(bumps(&repo, id).await, 0);
        }

        #[tokio::test]
        async fn a_server_failure_is_retried_and_the_retry_commits_once() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let id = concat!($backend, "-flaky");
            let message = command(BUMP, "msg-1", id, "flaky");
            let error = deliver(&service, &message).await.unwrap_err();
            assert!(error.is_retryable(), "{error}");
            assert_eq!(bumps(&repo, id).await, 0);
            deliver(&service, &message).await.unwrap();
            deliver(&service, &message).await.unwrap();
            assert_eq!(runs(id), 2, "one failed run, one committed run");
            assert_eq!(bumps(&repo, id).await, 1);
        }

        #[tokio::test]
        async fn malformed_input_is_refused_permanently() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let wrong_shape =
                Message::new(BUMP, MessageKind::Command, b"{\"id\":7}".to_vec()).with_id("msg-1");
            let error = deliver(&service, &wrong_shape).await.unwrap_err();
            assert!(error.is_permanent(), "{error}");
            let mut binary =
                Message::new(BUMP, MessageKind::Command, vec![0xff, 0x00]).with_id("msg-2");
            binary.content_type = "application/octet-stream".into();
            let error = deliver(&service, &binary).await.unwrap_err();
            assert!(error.is_permanent(), "{error}");
        }

        /// The whole consumer loop: an in-memory bus delivers the service's
        /// commands until its queues are empty.
        #[tokio::test]
        async fn a_bus_consumer_loop_delivers_typed_commands_through_the_ledger() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let bus = InMemoryBus::new();
            let dead = InMemoryDeadLetters::new();

            // At-least-once: the same command is queued twice. One command
            // fails once and is redelivered. Two are not for a bus to run.
            for message in [
                command(BUMP, "msg-1", concat!($backend, "-loop"), "ok"),
                command(BUMP, "msg-1", concat!($backend, "-loop"), "ok"),
                command(BUMP, "msg-2", concat!($backend, "-loop-flaky"), "flaky"),
                command(OPEN_BUMP, "msg-3", concat!($backend, "-loop-open"), "ok"),
                command(BUMP, "msg-4", concat!($backend, "-loop-rejected"), "reject"),
            ] {
                bus.send_message(message).await.unwrap();
            }

            service(repo.clone())
                .with_bus(bus.clone())
                .run(RunOptions::until_idle().with_dead_letters(Arc::new(dead.clone())))
                .await
                .unwrap();

            assert_eq!(bumps(&repo, concat!($backend, "-loop")).await, 1);
            assert_eq!(runs(concat!($backend, "-loop")), 1);
            assert_eq!(bumps(&repo, concat!($backend, "-loop-flaky")).await, 1);
            assert_eq!(runs(concat!($backend, "-loop-flaky")), 2);
            assert_eq!(bumps(&repo, concat!($backend, "-loop-open")).await, 0);
            assert_eq!(runs(concat!($backend, "-loop-open")), 0);
            assert_eq!(bumps(&repo, concat!($backend, "-loop-rejected")).await, 0);
            let mut dead_ids: Vec<String> = dead
                .all()
                .iter()
                .map(|dead| dead.message.id().unwrap().to_string())
                .collect();
            dead_ids.sort();
            assert_eq!(dead_ids, ["msg-3", "msg-4"]);
        }

        /// The Knative ingress: a CloudEvent marked as a command.
        #[cfg(feature = "http")]
        #[tokio::test]
        async fn the_knative_ingress_delivers_typed_commands_through_the_ledger() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/", listener.local_addr().unwrap());
            let app =
                distributed::microsvc::cloud_events_router(Arc::new(service(repo.clone())));
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let client = reqwest::Client::new();
            let post = |name: &'static str, id: &'static str, counter: &'static str| {
                client
                    .post(&url)
                    .header("ce-specversion", "1.0")
                    .header("ce-id", id)
                    .header("ce-type", name)
                    .header("ce-source", "/tests")
                    .header("ce-sourcedkind", "command")
                    .header("ce-x-user-id", "admin")
                    .header("ce-x-roles", "admin")
                    .header("content-type", "application/json")
                    .body(json!({ "id": counter, "mode": "ok" }).to_string())
                    .send()
            };

            let first = post(BUMP, "ce-1", concat!($backend, "-knative")).await.unwrap();
            assert_eq!(first.status(), 200);
            let first: serde_json::Value = first.json().await.unwrap();
            assert_eq!(first["count"], 1);
            assert_eq!(first["user"], TRANSPORT_USER_ID);
            assert_eq!(first["message_id"], "ce-1");

            // Knative redelivers: 200 again, nothing new committed.
            let again = post(BUMP, "ce-1", concat!($backend, "-knative")).await.unwrap();
            assert_eq!(again.status(), 200);
            assert_eq!(again.json::<serde_json::Value>().await.unwrap(), first);
            assert_eq!(runs(concat!($backend, "-knative")), 1);
            assert_eq!(bumps(&repo, concat!($backend, "-knative")).await, 1);

            // Not deliverable by a transport: permanent, so Knative stops.
            for name in [ADMIN_BUMP, OPEN_BUMP] {
                let refused = post(name, "ce-2", concat!($backend, "-knative-refused")).await.unwrap();
                assert_eq!(refused.status(), 422, "{name}");
            }
            assert_eq!(bumps(&repo, concat!($backend, "-knative-refused")).await, 0);
        }
    };
}

mod in_memory {
    use super::*;
    use distributed::InMemoryRepository;

    pub struct Db(InMemoryRepository);

    impl Db {
        pub async fn repo(&self) -> InMemoryRepository {
            self.0.clone()
        }
    }

    async fn fresh() -> Db {
        Db(InMemoryRepository::new())
    }

    scenarios!("memory", InMemoryRepository, fresh());
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use distributed::SqliteRepository;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DB: AtomicU64 = AtomicU64::new(0);

    /// A temporary database file, removed with its sidecars on drop.
    pub struct Db(std::path::PathBuf);

    impl Db {
        pub async fn repo(&self) -> SqliteRepository {
            SqliteRepository::connect_and_migrate(&format!(
                "sqlite://{}?mode=rwc",
                self.0.display()
            ))
            .await
            .unwrap()
        }
    }

    impl Drop for Db {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
            }
        }
    }

    async fn fresh() -> Db {
        Db(std::env::temp_dir().join(format!(
            "distributed-transport-typed-commands-{}-{}.db",
            std::process::id(),
            NEXT_DB.fetch_add(1, Ordering::Relaxed)
        )))
    }

    scenarios!("sqlite", SqliteRepository, fresh());
}
