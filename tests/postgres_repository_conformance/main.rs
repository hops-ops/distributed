#![cfg(feature = "postgres")]

#[path = "../persistent_repository_conformance/mod.rs"]
mod conformance;
#[path = "../support/postgres.rs"]
mod postgres;

use distributed::PostgresRepository;

async fn repository() -> Option<(postgres::PostgresTestSchema, PostgresRepository)> {
    let schema = postgres::PostgresTestSchema::create_from_env(
        "pg_conformance",
        "Postgres conformance test",
    )
    .await?;
    let repo = schema.repository().await;
    repo.bootstrap_table_schema_for_dev(&conformance::read_models::conformance_table_registry())
        .await
        .expect("read-model conformance table should bootstrap");
    Some((schema, repo))
}

conformance::repository_conformance_tests!();
