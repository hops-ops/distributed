//! Identity module: Zitadel ingress/scrape commands + the auth_users
//! projection.
//!
//! No aggregate: the commands publish user profiles as external facts, which
//! commit with their ledger receipts; the `AUTH_USERS` projection writes them.

use distributed::graphql::SurfaceProjector;
use distributed::microsvc::{ConfigurableOutboxPublisher, HasOutboxStore, HasRepo, Routes};
use distributed::AggregateRepository;

use crate::bounds::{EventStore, ReadStore};
use crate::deps::AuthDeps;
use crate::handlers::ingestors::{zitadel, zitadel_scrape};

/// Logical module id for composition inventories.
pub const MODULE_ID: &str = "identity";

/// Prefix of this module's command names (they are named for the provider).
pub const COMMAND_PREFIX: &str = "zitadel.";

/// Mount the Zitadel ingress and scrape commands and the auth_users
/// projection.
pub fn routes<R, S>(
    repo: R,
    read_models: S,
    auth_projector: SurfaceProjector,
) -> Routes<AuthDeps<R, S>>
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
        .consume_projection(auth_projector)
}

#[cfg(test)]
mod tests {
    use distributed::bus::{InMemoryBus, RunOptions};
    use distributed::{
        ReadModelWorkspaceExt, RowKey, RowValue, SqliteLockManager, SqliteRepository,
    };
    use e2e_readmodels::AuthUsers;
    use serde_json::json;

    use crate::handlers::ingestors::zitadel::{
        ingestor_principal, ingestor_session, ingress_command_id, normalize_ingress_body, COMMAND,
    };
    use crate::modules::compose::build_service;

    #[tokio::test]
    async fn an_ingested_profile_reaches_auth_users_through_the_projection() {
        let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
            .await
            .unwrap();
        let registry = crate::distributed_manifest().table_registry().unwrap();
        repo.bootstrap_table_schema_for_dev(&registry)
            .await
            .unwrap();
        let locks = SqliteLockManager::new(repo.pool().clone());
        let bus = InMemoryBus::new();
        let service =
            || build_service(repo.clone(), locks.clone(), repo.clone()).with_bus(bus.clone());

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

        let row = repo
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
