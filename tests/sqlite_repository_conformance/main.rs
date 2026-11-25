#![cfg(feature = "sqlite")]

#[path = "../persistent_repository_conformance/mod.rs"]
mod conformance;

use distributed::SqliteRepository;

async fn repository() -> Option<((), SqliteRepository)> {
    let repo = SqliteRepository::connect_and_migrate("sqlite::memory:")
        .await
        .expect("sqlite conformance repository should migrate");
    repo.bootstrap_table_schema_for_dev(&conformance::read_models::conformance_table_registry())
        .await
        .expect("read-model conformance table should bootstrap");
    Some(((), repo))
}

conformance::repository_conformance_tests!();
