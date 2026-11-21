//! Identity module: Zitadel ingress/scrape commands + auth_users projector.
//!
//! No aggregate: the commands publish provider messages, which commit with
//! their ledger receipts.

use distributed::microsvc::{ConfigurableOutboxPublisher, HasOutboxStore, HasRepo, Routes};
use distributed::AggregateRepository;

use crate::bounds::{EventStore, ReadStore};
use crate::deps::AuthDeps;
use crate::handlers::events::project_auth_user;
use crate::handlers::ingestors::{zitadel, zitadel_scrape};

/// Logical module id for composition inventories.
pub const MODULE_ID: &str = "identity";

/// Prefix of this module's command names (they are named for the provider).
pub const COMMAND_PREFIX: &str = "zitadel.";

/// Mount the Zitadel ingress and scrape commands and the auth_users projector.
pub fn routes<R, S>(repo: R, read_models: S) -> Routes<AuthDeps<R, S>>
where
    R: EventStore,
    S: ReadStore,
    AggregateRepository<R, ()>:
        HasRepo + HasOutboxStore + ConfigurableOutboxPublisher + Send + Sync + 'static,
{
    let scrape = zitadel_scrape::Scrape(read_models.clone());
    Routes::for_stores(repo, read_models)
        .typed_command(zitadel::command())
        .handle(zitadel::handle)
        .typed_command(zitadel_scrape::command())
        .handle(scrape)
        .events(project_auth_user::EVENTS)
        .guarded(project_auth_user::guard, project_auth_user::handle)
}
