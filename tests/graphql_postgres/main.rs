//! Env-gated Postgres GraphQL smoke suite.
//! Needs DATABASE_URL; see `tests/support/env.rs` for what happens without it.

#![cfg(all(feature = "graphql", feature = "postgres"))]

#[path = "../support/env.rs"]
mod env_support;

use async_graphql::Request;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions};
use distributed::microsvc::{Session, ROLE_KEY};
use distributed::ReadModel;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("gql_pg_smoke")]
struct SmokeView {
    #[id("id")]
    id: String,
    label: String,
}

#[tokio::test]
async fn postgres_list_query_when_database_url_set() {
    let Some(url) = env_support::require_service("DATABASE_URL", "Postgres GraphQL list query")
    else {
        return;
    };

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect postgres");

    sqlx::query("DROP TABLE IF EXISTS gql_pg_smoke")
        .execute(&pool)
        .await
        .ok();
    sqlx::query("CREATE TABLE gql_pg_smoke (id TEXT PRIMARY KEY, label TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("seed");
    sqlx::query("INSERT INTO gql_pg_smoke VALUES ('1', 'hello')")
        .execute(&pool)
        .await
        .expect("seed");

    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<SmokeView>(ModelPermissions::new().grant("user", read().all_columns()))
        .build()
        .expect("build");

    let mut session = Session::new();
    session.set(ROLE_KEY, "user");
    let resp = engine
        .execute(&session, Request::new("{ gql_pg_smoke { id label } }"))
        .await;
    assert!(!resp.is_err(), "{:?}", resp.errors);
    let data = serde_json::to_value(&resp.data).unwrap();
    assert_eq!(data["gql_pg_smoke"][0]["label"], "hello");
}
