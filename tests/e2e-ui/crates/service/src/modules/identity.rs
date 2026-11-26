//! Identity module: Zitadel ingress/scrape commands + the auth_users
//! projection.
//!
//! No aggregate: the commands publish user profiles as external facts, which
//! commit with their ledger receipts; the `AUTH_USERS` projection writes them.

use distributed::microsvc::{Routes, ServiceStores, StoreDependencies};
use distributed::projection::LocalProjectionMounts;
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

#[cfg(test)]
mod tests {
    use distributed::bus::{InMemoryBus, RunOptions};
    use distributed::microsvc::{SqlStores, SqliteStores};
    use distributed::read_model::ReadModelWorkspaceExt;
    use distributed::table::{RowKey, RowValue};
    use e2e_readmodels::AuthUsers;
    use serde_json::json;

    use crate::handlers::ingestors::zitadel::{
        ingestor_principal, ingestor_session, ingress_command_id, normalize_ingress_body, COMMAND,
    };
    use crate::modules::compose::build_service;

    #[tokio::test]
    async fn an_ingested_profile_reaches_auth_users_through_the_projection() {
        let stores = SqliteStores::connect("sqlite::memory:").await.unwrap();
        stores
            .bootstrap_read_models(e2e_readmodels::read_models().catalog())
            .await
            .unwrap();
        let bus = InMemoryBus::new();
        let service = || build_service(&stores).with_bus(bus.clone());

        let input = normalize_ingress_body(&json!({
            "event_id": "evt-1",
            "event_type": "user.human.deactivated",
            "user_id": "sub-1",
            "email": "ada@example.com",
        }));
        service()
            .dispatch_causal(
                COMMAND,
                &ingress_command_id(&input),
                serde_json::to_value(&input).unwrap(),
                ingestor_session(),
                ingestor_principal(),
            )
            .await
            .unwrap();
        service().run(RunOptions::until_idle()).await.unwrap();

        let row = stores
            .read_models
            .workspace()
            .load::<AuthUsers>(RowKey::new([("user_id", RowValue::String("sub-1".into()))]))
            .one()
            .await
            .unwrap()
            .expect("the projection wrote the profile")
            .data;
        assert_eq!(row.email, "ada@example.com");
        assert_eq!(row.display_name, "ada@example.com");
        assert_eq!(row.status, "deactivated");
        assert_eq!(row.approval_status, "pending");
    }
}
