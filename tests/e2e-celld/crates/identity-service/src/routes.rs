//! Zitadel ingress/scrape commands + the AuthUsers projection.
//!
//! No aggregate: the commands publish user profiles as external facts, which
//! commit with their ledger receipts; the `AUTH_USERS` projection writes them.

use distributed::microsvc::{Routes, ServiceStores, StoreDependencies};
use distributed::LocalProjectionMounts;
use e2e_projections::AUTH_USERS;

use crate::handlers::ingestors::{zitadel, zitadel_scrape};

/// Mount the Zitadel ingress and scrape commands and the auth_users
/// projection.
pub fn routes<St: ServiceStores>(
    stores: &St,
    projections: &LocalProjectionMounts,
) -> Routes<StoreDependencies<St>> {
    let scrape = zitadel_scrape::Scrape(stores.read_models().clone());
    Routes::for_stores(stores.events().clone(), stores.read_models().clone())
        .typed_command(zitadel::command())
        .handle(zitadel::handle)
        .typed_command(zitadel_scrape::command())
        .handle(scrape)
        .consume_projection(projections.projector(AUTH_USERS))
}
