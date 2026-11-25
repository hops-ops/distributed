//! Compose bounded-context modules into one e2e-ui Service.

use blob_domain::BlobGame;
use chat_domain::ChatMessage;
use distributed::microsvc::{ConfigurableOutboxPublisher, HasOutboxStore, HasRepo, Service};
use distributed::{AggregateBuilder, AggregateRepository, QueuedRepository};
use todo_domain::Todo;

use crate::bounds::{EventStore, Locks, ReadStore};
use crate::modules::projections;
use e2e_celld_blob as blob;
use e2e_celld_chat as chat;
use e2e_celld_identity as identity;
use e2e_celld_identity::handlers::ingestors::{zitadel, zitadel_scrape};
use e2e_celld_todo as todo;

/// Explicit module inventory for the celld example (same ids as e2e-ui so the UI client matches).
pub const MODULE_IDS: &[&str] = &[
    todo::MODULE_ID,
    chat::MODULE_ID,
    blob::MODULE_ID,
    identity::MODULE_ID,
];

/// Compose todo + chat + blob + identity into one Service.
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
    let identity = identity::routes(repo, read_models, projections.auth);

    // GraphQL-only public write surface. POST /todo.* stays 404 (suite T0).
    // Zitadel Action ingress is the identity crate: its two commands are
    // exposed through edges that check the shared secret and run as the
    // ingestor (`http::serve` also requires the cell internal secret).
    Service::new()
        .named("e2e-ui")
        .routes(todos)
        .routes(chat)
        .routes(blob)
        .routes(identity)
        .with_http_command(zitadel::COMMAND, zitadel::http_edge)
        .with_http_command(zitadel_scrape::COMMAND, zitadel_scrape::http_edge)
}
