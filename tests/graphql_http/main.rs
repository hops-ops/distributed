//! HTTP GraphiQL on/off + session role integration tests.

#![cfg(all(feature = "graphql", feature = "sqlite"))]

use std::sync::Arc;

use distributed::graphql::{graphiql_enabled_from_env_vars, read, GraphqlEngine, ModelPermissions};
use distributed::microsvc::{router, Service};
use distributed::ReadModel;
use serde::{Deserialize, Serialize};
use tower::util::ServiceExt;

#[path = "../support/graphql.rs"]
mod graphql_support;
use graphql_support::Db;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
#[table("http_items")]
struct HttpItem {
    #[id("id")]
    id: String,
    name: String,
}

async fn service_with_graphiql(on: bool) -> Arc<Service> {
    let db = Db::sqlite().await;
    db.seed([HttpItem {
        id: "1".into(),
        name: "n".into(),
    }])
    .await;
    let pool = db.pool();
    let engine = GraphqlEngine::builder(pool)
        .service_id("http-gql")
        .roles(&["user"])
        .model::<HttpItem>(ModelPermissions::new().grant("user", read().all_columns()))
        .graphiql(on)
        .build()
        .unwrap();
    Arc::new(Service::new().named("http-gql").with_graphql(engine))
}

#[tokio::test]
async fn graphiql_get_200_when_enabled() {
    let svc = service_with_graphiql(true).await;
    let app = router(svc);
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/graphql")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body = String::from_utf8_lossy(&bytes);
    assert!(
        body.contains("GraphiQL") || body.contains("graphiql") || body.contains("/graphql"),
        "unexpected body: {body}"
    );
}

#[tokio::test]
async fn graphiql_get_405_when_disabled() {
    let svc = service_with_graphiql(false).await;
    let app = router(svc);
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/graphql")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn post_graphql_with_role_returns_data() {
    let svc = service_with_graphiql(false).await;
    let app = router(svc);
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .header("x-roles", "user")
                .body(axum::body::Body::from(
                    r#"{"query":"{ http_items { id name } }"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), axum::http::StatusCode::OK);
    let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        v["data"]["http_items"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false),
        "response: {v}"
    );
}

/// harden-11: production-like env policy disables GraphiQL on the real HTTP path.
///
/// Drives shipped `graphiql_enabled_from_env_vars` (same function scaffold calls via
/// `graphiql_enabled_from_env`) then mounts the engine with that boolean.
#[tokio::test]
async fn production_env_policy_disables_graphiql_http_get() {
    // Pure policy (no process-env mutation): RUST_ENV=production → graphiql false.
    let graphiql = graphiql_enabled_from_env_vars(None, Some("production"), None, None);
    assert!(
        !graphiql,
        "RUST_ENV=production must disable GraphiQL in shipped policy"
    );

    let svc = service_with_graphiql(graphiql).await;
    let app = router(svc);
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri("/graphql")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        res.status(),
        axum::http::StatusCode::METHOD_NOT_ALLOWED,
        "production policy must yield GET /graphql 405 (GraphiQL off)"
    );
}
