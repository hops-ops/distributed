//! Compose bounded-context modules into one e2e-ui Service.

use distributed::microsvc::{Service, ServiceStores};

use crate::modules::projections::projections;
use crate::E2E_UI_APPLICATION;
use e2e_celld_blob as blob;
use e2e_celld_chat as chat;
use e2e_celld_identity as identity;
use e2e_celld_identity::handlers::ingestors::{zitadel, zitadel_scrape};
use e2e_celld_todo as todo;

/// Compose todo + chat + blob + identity into one Service.
///
/// This is the review-visible application wiring: list modules, do not invent
/// infrastructure. The application's modules are the command namespaces
/// (`todo.*`, `chat.*`, `blob.*`, `zitadel.*`); the host lives in `host`.
pub fn build_service<St: ServiceStores>(stores: &St) -> Service {
    let projections = projections();

    // GraphQL-only public write surface. POST /todo.* stays 404 (suite T0).
    // Zitadel Action ingress is the identity crate: its two commands are
    // exposed through edges that check the shared secret and run as the
    // ingestor (`http::serve` also requires the cell internal secret).
    Service::new()
        .named(E2E_UI_APPLICATION)
        .routes(todo::routes(stores, &projections))
        .routes(chat::routes(stores, &projections))
        .routes(blob::routes(stores))
        .routes(identity::routes(stores, &projections))
        .with_http_command(zitadel::COMMAND, zitadel::http_edge)
        .with_http_command(zitadel_scrape::COMMAND, zitadel_scrape::http_edge)
}
