//! Compose bounded-context modules into one e2e-ui Service.

use blob_domain::BlobGame;
use chat_domain::ChatMessage;
use distributed::microsvc::{ConfigurableOutboxPublisher, HasOutboxStore, HasRepo, Service};
use distributed::{AggregateBuilder, AggregateRepository, QueuedRepository};
use todo_domain::Todo;

use crate::bounds::{EventStore, Locks, ReadStore};
use crate::modules::{blob, chat, identity, projections, todo};

/// Explicit module inventory for the e2e-ui application.
pub const MODULE_IDS: &[&str] = &[
    todo::MODULE_ID,
    chat::MODULE_ID,
    blob::MODULE_ID,
    identity::MODULE_ID,
];

/// Compose todo + chat + blob + identity modules into one Service.
///
/// This is the review-visible application wiring: list modules, do not invent
/// infrastructure. Dialect runners and workers live in `host`.
pub fn build_service<R, L, S>(repo: R, locks: L, read_models: S) -> Service
where
    R: EventStore,
    L: Locks,
    S: ReadStore,
    QueuedRepository<R, L>: Clone
        + AggregateBuilder
        + HasOutboxStore
        + distributed::TransactionalCommit
        + Send
        + Sync
        + 'static,
    AggregateRepository<QueuedRepository<R, L>, Todo>:
        HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
    AggregateRepository<QueuedRepository<R, L>, ChatMessage>:
        HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
    AggregateRepository<QueuedRepository<R, L>, BlobGame>:
        HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
    AggregateRepository<R, ()>:
        HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
{
    let projections = projections::projection_owners();
    let todos = todo::routes(
        repo.clone(),
        locks.clone(),
        read_models.clone(),
        projections.todo,
    );
    let chat = chat::routes(
        repo.clone(),
        locks.clone(),
        read_models.clone(),
        projections.chat,
    );
    let blob = blob::routes(repo.clone(), locks, read_models.clone(), projections.blob);
    let identity = identity::routes(repo, read_models);

    // GraphQL-only public write surface. POST /todo.* stays 404 (suite T0).
    // Zitadel Action ingress still needs HTTP: the identity module's commands
    // get an authenticating edge in `http::gateway_router`.
    Service::new()
        .named("e2e-ui")
        .routes(todos)
        .routes(chat)
        .routes(blob)
        .routes(identity)
}
