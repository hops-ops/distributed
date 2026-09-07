//! T* Transport / surface red-team (engine + HTTP).

use std::sync::Arc;

use async_graphql::Request;
use distributed::command::{typed_command, PreparedCommand, Succeeded};
use distributed::graphql::{graphiql_enabled_from_env_vars, read, GraphqlEngine, ModelPermissions};
use distributed::microsvc::{router, CausalCommandContext, HandlerError, Routes, Service, Session};
use distributed::{
    Aggregate, AggregateRepository, Entity, EventRecord, InMemoryRepository, ReadModel,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqlitePoolOptions;
use tower::util::ServiceExt;

use super::common::{seed_orders, session, OrderView};

#[derive(Default)]
struct T4Aggregate {
    entity: Entity,
}

impl Aggregate for T4Aggregate {
    type ReplayError = String;

    fn aggregate_type() -> &'static str {
        "graphql-harden-t4"
    }

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

#[derive(Deserialize, distributed::CommandInput)]
struct T4CommandInput {
    id: String,
    name: String,
}

#[derive(Serialize, distributed::CommandOutput)]
struct T4CommandOutput {
    id: String,
    name: String,
}

async fn t4_command_handler(
    _context: &CausalCommandContext<'_, T4Aggregate>,
    input: T4CommandInput,
) -> Result<PreparedCommand<Succeeded<T4CommandOutput>>, HandlerError> {
    Ok(
        PreparedCommand::<Succeeded<T4CommandOutput>>::prepare(T4CommandOutput {
            id: input.id,
            name: input.name,
        })
        .expect("fixture output is serializable"),
    )
}

/// T3a: anonymous introspection disabled at engine level (existing harden-12).
#[tokio::test]
async fn anonymous_introspection_disabled_when_flag_false() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user", "anonymous"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .introspection_for_anonymous(false)
        .build()
        .unwrap();

    let anon = Session::new();
    let resp = engine
        .execute(&anon, Request::new("{ __schema { queryType { name } } }"))
        .await;
    assert!(
        !resp.errors.is_empty(),
        "anonymous introspection should fail when flag is false"
    );

    let user = session("user", "u1");
    let resp = engine
        .execute(&user, Request::new("{ __schema { queryType { name } } }"))
        .await;
    assert!(
        resp.errors.is_empty(),
        "user introspection: {:?}",
        resp.errors
    );
    let data = resp.data.into_json().unwrap();
    assert_eq!(
        data["__schema"]["queryType"]["name"].as_str(),
        Some("Query")
    );
}

#[tokio::test]
async fn anonymous_introspection_allowed_when_flag_true() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user", "anonymous"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .introspection_for_anonymous(true)
        .build()
        .unwrap();
    let resp = engine
        .execute(
            &Session::new(),
            Request::new("{ __schema { queryType { name } } }"),
        )
        .await;
    assert!(resp.errors.is_empty(), "{:?}", resp.errors);
}

/// T3b: introspection over HTTP with role headers.
#[tokio::test]
async fn t3_introspection_over_http_respects_role() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .service_id("rt-http")
        .roles(&["user", "anonymous"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .introspection_for_anonymous(false)
        .graphiql(false)
        .build()
        .unwrap();
    let svc = Arc::new(Service::new().named("rt-http").with_graphql(engine));
    let app = router(svc);

    // Anonymous POST introspection → error
    let res = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    r#"{"query":"{ __schema { queryType { name } } }"}"#,
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
        v.get("errors")
            .map(|e| !e.as_array().unwrap().is_empty())
            .unwrap_or(false),
        "anon HTTP introspection should error: {v}"
    );

    // Authenticated role can introspect
    let res = app
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .header("x-roles", "user")
                .body(axum::body::Body::from(
                    r#"{"query":"{ __schema { queryType { name } } }"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(res.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(
        v.get("errors")
            .map(|e| e.as_array().unwrap().is_empty())
            .unwrap_or(true),
        "user HTTP introspection should succeed: {v}"
    );
    assert_eq!(v["data"]["__schema"]["queryType"]["name"], "Query");
}

/// T4: mutation field absent for role without command grant.
#[tokio::test]
async fn t4_mutation_absent_without_command_grant() {
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ReadModel)]
    #[table("t4_items")]
    struct Item {
        #[id("id")]
        id: String,
        name: String,
    }

    let pool = SqlitePoolOptions::new()
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE t4_items (id TEXT PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO t4_items VALUES ('1', 'n');",
    )
    .execute(&pool)
    .await
    .unwrap();

    let routes: Routes<AggregateRepository<InMemoryRepository, T4Aggregate>> = Routes::new()
        .with_repo(AggregateRepository::new(InMemoryRepository::new()))
        .typed_command(
            typed_command::<T4CommandInput, Succeeded<T4CommandOutput>>("item.create")
                .field_name("createItem")
                .roles(["admin"]),
        )
        .handle(t4_command_handler);
    let service = Service::new().named("graphql-harden-t4").routes(routes);

    let engine = GraphqlEngine::builder(pool)
        .service(&service)
        .protocol_token_key([0x44; 32])
        .roles(&["user", "admin"])
        .model::<Item>(
            ModelPermissions::new()
                .grant("user", read().all_columns())
                .grant("admin", read().all_columns()),
        )
        .build()
        .unwrap();

    // user: mutation createItem must not be available
    let user = session("user", "u");
    let resp = engine
        .execute(
            &user,
            Request::new(
                r#"mutation { createItem(commandId: "019bde25-03a7-7cc5-a8f0-627d2f540001", input: { id: "2", name: "x" }) { id } }"#,
            ),
        )
        .await;
    assert!(
        !resp.errors.is_empty(),
        "user must not invoke admin-only mutation: {:?}",
        resp.data
    );
    let msgs = resp
        .errors
        .iter()
        .map(|e| e.message.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        msgs.contains("createitem")
            || msgs.contains("mutation")
            || msgs.contains("unknown")
            || msgs.contains("field"),
        "expected unknown mutation field for ungranted role, got {msgs}"
    );
}

/// Production GraphiQL policy still drives HTTP 405 when off.
#[tokio::test]
async fn production_env_policy_disables_graphiql_http_get() {
    let graphiql = graphiql_enabled_from_env_vars(None, Some("production"), None, None);
    assert!(!graphiql);

    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .service_id("prod-gql")
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .graphiql(graphiql)
        .build()
        .unwrap();
    let svc = Arc::new(Service::new().named("prod-gql").with_graphql(engine));
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

#[cfg(feature = "metrics")]
#[tokio::test]
async fn graphql_metrics_increment_on_execute() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .build()
        .unwrap();
    let s = session("user", "x");
    let _ = engine
        .execute(&s, Request::new("{ orders { order_id } }"))
        .await;
    let text = distributed::metrics::prometheus_text();
    assert!(
        text.contains("distributed_graphql_request_total"),
        "metrics text missing graphql series: {text}"
    );
    assert!(
        text.contains("status=\"ok\"") || text.contains("status=\\\"ok\\\""),
        "ok path should label status=ok: {text}"
    );
}

/// BAD_REQUEST from max_bool_width should label metrics status=bad_request (when code present).
#[cfg(feature = "metrics")]
#[tokio::test]
async fn graphql_metrics_bad_request_status_label() {
    let pool = seed_orders().await;
    let engine = GraphqlEngine::builder(pool)
        .roles(&["user"])
        .model::<OrderView>(ModelPermissions::new().grant("user", read().all_columns()))
        .max_bool_width(2)
        .build()
        .unwrap();
    let s = session("user", "x");
    let wide = (0..5)
        .map(|i| format!(r#"{{ status: {{ _eq: "open{i}" }} }}"#))
        .collect::<Vec<_>>()
        .join(", ");
    let q = format!(r#"{{ orders(where: {{ _or: [{wide}] }}) {{ order_id }} }}"#);
    let resp = engine.execute(&s, Request::new(q)).await;
    assert!(!resp.errors.is_empty(), "expected bool width rejection");
    let text = distributed::metrics::prometheus_text();
    assert!(
        text.contains("status=\"bad_request\"")
            || text.contains("bad_request")
            || text.contains("status=\"error\""),
        "expected bad_request (or error fallback) metric status, got excerpt: {}",
        text.lines()
            .filter(|l| l.contains("graphql"))
            .take(8)
            .collect::<Vec<_>>()
            .join("\n")
    );
}
