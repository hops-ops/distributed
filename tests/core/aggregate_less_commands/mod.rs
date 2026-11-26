//! Typed commands on a route bundle with no aggregate
//! ([`Routes::for_stores`]): a provider webhook, a reconciliation.
//!
//! They run the pipeline every typed command runs. What a handler stages (an
//! outbox message, an external fact) commits with the command's ledger
//! receipt in one transaction, and no stream is written:
//!
//! - a second dispatch of one command id replays and stages nothing more;
//! - a refusal is recorded and replayed, with nothing staged;
//! - a commit that fails records no receipt;
//! - a command that stages nothing succeeds;
//! - an external fact (`publish_external`) is published as a domain-event
//!   occurrence, identified by its source and caused by the command.
//!
//! The scenarios run on the in-memory repository and, with `sqlite`, on a
//! SQLite file.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::json;

use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::command_dispatch::{deterministic_command_id, VerifiedPrincipal};
use distributed::microsvc::{
    CausalCommandContext, CausalDispatchError, CausalDispatchResult, HandlerError, HasOutboxStore,
    Routes, Service, Session, ROLE_KEY, USER_ID_KEY,
};
use distributed::{ExternalEventSource, OutboxMessage, OutboxStore};

const RECEIVE: &str = "webhook.receive";
const SUBJECT: &str = "system:webhook";
const ROLE: &str = "webhook";

#[derive(Debug, Deserialize, distributed::CommandInput)]
struct ReceiveInput {
    /// The id of the outbox message the handler stages.
    delivery: String,
    /// `publish`, `external` (publishes [`WebhookReceived`] instead of a raw
    /// message), `nothing` or `reject`.
    mode: String,
}

/// The external fact the `external` mode publishes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, distributed::DomainEvent)]
#[domain_event(name = "webhook.received", version = 1)]
struct WebhookReceived {
    delivery: String,
}

/// Where the `external` mode's fact comes from: the provider's delivery.
fn webhook_source(delivery: &str) -> ExternalEventSource {
    ExternalEventSource {
        producer: RECEIVE.into(),
        stream: delivery.into(),
        position: 1,
        key: "delivery".into(),
    }
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
struct ReceivePayload {
    published: bool,
    command_id: String,
}

/// Handler runs per delivery (each test of each backend uses its own).
static RUNS: Mutex<Option<HashMap<String, usize>>> = Mutex::new(None);

fn runs(delivery: &str) -> usize {
    RUNS.lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .get(delivery)
        .copied()
        .unwrap_or(0)
}

async fn receive(
    ctx: &CausalCommandContext<'_, ()>,
    input: ReceiveInput,
) -> Result<PreparedCommand<Succeeded<ReceivePayload>>, HandlerError> {
    *RUNS
        .lock()
        .unwrap()
        .get_or_insert_with(HashMap::new)
        .entry(input.delivery.clone())
        .or_default() += 1;
    let command_id = ctx.message_id().unwrap_or_default().to_string();
    let payload = |published| ReceivePayload {
        published,
        command_id: command_id.clone(),
    };
    let message = || {
        OutboxMessage::encode(
            input.delivery.clone(),
            "webhook.received.v1",
            &json!({ "delivery": input.delivery }),
        )
        .map_err(|error| HandlerError::Other(Box::new(error)))
    };
    match input.mode.as_str() {
        "reject" => Err(HandlerError::Rejected("not a delivery we accept".into())),
        "external" => ctx
            .publish_external(
                webhook_source(&input.delivery),
                WebhookReceived {
                    delivery: input.delivery.clone(),
                },
            )
            .commit()?
            .succeeded(payload(true)),
        "nothing" => PreparedCommand::prepare(payload(false))
            .map_err(|error| HandlerError::Other(Box::new(error))),
        _ => ctx.outbox(message()?).commit()?.succeeded(payload(true)),
    }
}

fn service<R>(repo: R) -> Service
where
    R: distributed::microsvc::HasReadModelStore + Clone + Send + Sync + 'static,
    distributed::AggregateRepository<R, ()>: distributed::microsvc::HasRepo
        + HasOutboxStore
        + distributed::microsvc::ConfigurableOutboxPublisher
        + Send
        + Sync
        + 'static,
    distributed::microsvc::RepoReadModelDependencies<distributed::AggregateRepository<R, ()>, R>:
        distributed::microsvc::CausalRouteDependencies<Aggregate = ()>,
{
    Service::new().named("webhooks").routes(
        Routes::for_stores(repo.clone(), repo)
            .typed_command(
                typed_command::<ReceiveInput, Succeeded<ReceivePayload>>(RECEIVE).roles([ROLE]),
            )
            .handle(receive),
    )
}

async fn dispatch(
    service: &Service,
    command_id: &str,
    delivery: &str,
    mode: &str,
) -> Result<serde_json::Value, CausalDispatchError> {
    dispatch_with_receipt(service, command_id, delivery, mode)
        .await
        .map(|result| result.payload().clone())
}

async fn dispatch_with_receipt(
    service: &Service,
    command_id: &str,
    delivery: &str,
    mode: &str,
) -> Result<CausalDispatchResult, CausalDispatchError> {
    let mut session = Session::new();
    session.set(USER_ID_KEY, SUBJECT);
    session.set(ROLE_KEY, ROLE);
    service
        .dispatch_causal_with_receipt(
            RECEIVE,
            command_id,
            json!({ "delivery": delivery, "mode": mode }),
            session,
            VerifiedPrincipal::internal(SUBJECT).unwrap(),
        )
        .await
}

/// The command id of one delivery: a redelivery derives the same one.
fn command_id(delivery: &str) -> String {
    deterministic_command_id(&["webhook", delivery])
}

/// Ids of this delivery's rows in the outbox (no publisher is configured, so
/// a committed row stays pending).
async fn staged<S: OutboxStore>(store: &S, delivery: &str) -> usize {
    store
        .pending(usize::MAX)
        .await
        .unwrap()
        .iter()
        .filter(|message| message.id() == delivery)
        .count()
}

macro_rules! scenarios {
    ($backend:literal, $repo_ty:ty, $fresh:expr) => {
        #[tokio::test]
        async fn a_staged_message_commits_with_the_receipt_and_a_redelivery_replays() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let delivery = concat!($backend, "-once");
            let id = command_id(delivery);

            let first = dispatch(&service, &id, delivery, "publish").await.unwrap();
            assert_eq!(first, json!({ "published": true, "command_id": id }));
            assert_eq!(staged(&repo.outbox_store(), delivery).await, 1);

            let second = dispatch(&service, &id, delivery, "publish").await.unwrap();
            assert_eq!(second, first, "a redelivery returns the stored receipt");
            assert_eq!(runs(delivery), 1, "the handler ran once");
            assert_eq!(staged(&repo.outbox_store(), delivery).await, 1);

            // The same id for another input is refused, not run.
            let reused = dispatch(&service, &id, delivery, "nothing").await;
            assert!(
                matches!(reused, Err(CausalDispatchError::CommandIdReuse)),
                "{reused:?}"
            );
            assert_eq!(runs(delivery), 1);
        }

        #[tokio::test]
        async fn a_refusal_is_recorded_and_replayed_with_nothing_staged() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let delivery = concat!($backend, "-rejected");
            let id = command_id(delivery);

            for _delivery in 0..2 {
                match dispatch(&service, &id, delivery, "reject").await {
                    Err(CausalDispatchError::Rejected {
                        status, message, ..
                    }) => {
                        assert_eq!(status, 422);
                        assert!(message.contains("not a delivery we accept"), "{message}");
                    }
                    other => panic!("expected the recorded rejection, got {other:?}"),
                }
            }
            assert_eq!(runs(delivery), 1, "the stored rejection answers the retry");
            assert_eq!(staged(&repo.outbox_store(), delivery).await, 0);
        }

        #[tokio::test]
        async fn an_external_fact_is_an_occurrence_of_its_source_caused_by_the_command() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let delivery = concat!($backend, "-external");
            let id = command_id(delivery);

            let result = dispatch_with_receipt(&service, &id, delivery, "external")
                .await
                .unwrap();
            let again = dispatch_with_receipt(&service, &id, delivery, "external")
                .await
                .unwrap();
            assert_eq!(again.causation_id(), result.causation_id());
            assert_eq!(runs(delivery), 1);

            let published: Vec<_> = repo
                .outbox_store()
                .pending(usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .filter_map(|message| {
                    let occurrence = message.domain_event_occurrence().ok()?;
                    (occurrence.external_source() == Some(&webhook_source(delivery)))
                        .then_some((message, occurrence))
                })
                .collect();
            assert_eq!(published.len(), 1, "one occurrence per source");
            let (message, occurrence) = &published[0];
            assert_eq!(message.id(), occurrence.id());
            assert_eq!(message.event_type, "webhook.received");
            assert_eq!(occurrence.causation_id(), Some(result.causation_id()));
            assert_eq!(
                occurrence.decode_body::<WebhookReceived>().unwrap(),
                WebhookReceived {
                    delivery: delivery.into()
                }
            );

            // Another command publishing the same source fact is the typed
            // duplicate: its transaction fails whole, with no receipt.
            let other = command_id(concat!($backend, "-external-again"));
            let error = dispatch(&service, &other, delivery, "external")
                .await
                .unwrap_err();
            assert!(
                matches!(error, CausalDispatchError::Internal(_)),
                "{error:?}"
            );
        }

        #[tokio::test]
        async fn a_command_that_stages_nothing_succeeds() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let delivery = concat!($backend, "-nothing");
            let id = command_id(delivery);

            for _delivery in 0..2 {
                let payload = dispatch(&service, &id, delivery, "nothing").await.unwrap();
                assert_eq!(payload, json!({ "published": false, "command_id": id }));
            }
            assert_eq!(runs(delivery), 1);
            assert_eq!(staged(&repo.outbox_store(), delivery).await, 0);
        }

        #[tokio::test]
        async fn a_commit_that_fails_records_no_receipt() {
            let db = $fresh.await;
            let repo: $repo_ty = db.repo().await;
            let service = service(repo.clone());
            let delivery = concat!($backend, "-taken");
            dispatch(&service, &command_id(delivery), delivery, "publish")
                .await
                .unwrap();

            // Another command stages the outbox id the first one committed:
            // its transaction fails as a whole, so neither a second message
            // nor a receipt exists, and its retry runs the handler again.
            let other = command_id(concat!($backend, "-taken-again"));
            for attempt in 1..=2 {
                let error = dispatch(&service, &other, delivery, "publish")
                    .await
                    .unwrap_err();
                assert!(
                    matches!(error, CausalDispatchError::Internal(_)),
                    "{error:?}"
                );
                assert_eq!(runs(delivery), 1 + attempt);
            }
            assert_eq!(staged(&repo.outbox_store(), delivery).await, 1);
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
            let repo = SqliteRepository::connect_and_migrate(&format!(
                "sqlite://{}?mode=rwc",
                self.0.display()
            ))
            .await
            .unwrap();
            repo
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
            "distributed-aggregate-less-commands-{}-{}.db",
            std::process::id(),
            NEXT_DB.fetch_add(1, Ordering::Relaxed)
        )))
    }

    scenarios!("sqlite", SqliteRepository, fresh());
}
