//! One command exposed over HTTP through its own edge
//! (`Service::with_http_command`), every other command path closed.

use std::sync::Arc;

use axum::http::HeaderMap;
use distributed::command_dispatch::VerifiedPrincipal;
use distributed::microsvc::{
    self, CausalDispatchError, HandlerError, HttpCommandDispatch, Routes, Service, Session,
    USER_ID_KEY,
};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::{json, Value};

use crate::microsvc::commands::command_id;
use crate::microsvc::handlers;
use crate::microsvc::models::counter::Counter;

const SECRET_HEADER: &str = "x-hook-secret";
const HOOK_USER: &str = "system:hook";

/// The edge of `session.identify`: a shared secret, then the hook's own
/// identity. The command ID comes from the body.
fn hook_edge(headers: &HeaderMap, body: Value) -> Result<HttpCommandDispatch, CausalDispatchError> {
    if headers.get(SECRET_HEADER).and_then(|v| v.to_str().ok()) != Some("s3cret") {
        return Err(CausalDispatchError::Handler(HandlerError::Unauthorized(
            "bad hook secret".into(),
        )));
    }
    let command_id = body["delivery"]
        .as_str()
        .ok_or_else(|| CausalDispatchError::BadRequest("delivery is missing".into()))?
        .to_string();
    let mut session = Session::new();
    session.set(USER_ID_KEY, HOOK_USER);
    Ok(HttpCommandDispatch {
        command_id,
        input: json!({}),
        session,
        principal: VerifiedPrincipal::internal(HOOK_USER).unwrap(),
    })
}

/// `session.identify` exposed through [`hook_edge`]; `counter.initialize`
/// registered but not exposed.
fn hook_service() -> Arc<Service> {
    Arc::new(
        Service::new()
            .named("hooks")
            .routes(
                Routes::new()
                    .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
                    .typed_command(handlers::counter_create::command())
                    .handle(handlers::counter_create::handle)
                    .typed_command(handlers::whoami::command())
                    .handle(handlers::whoami::handle),
            )
            .with_http_command(handlers::whoami::COMMAND, hook_edge),
    )
}

async fn start_app(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn an_exposed_command_runs_as_its_edge_says_and_no_header_reaches_it() {
    let base = start_app(microsvc::router(hook_service())).await;
    let client = reqwest::Client::new();
    let delivery = command_id();
    let call = |secret: &str| {
        client
            .post(format!("{base}/{}", handlers::whoami::COMMAND))
            .header(SECRET_HEADER, secret)
            // A caller-chosen identity: the edge's session replaces it.
            .header("x-user-id", "attacker")
            .header("x-roles", "admin")
            .json(&json!({ "delivery": delivery }))
            .send()
    };

    let refused = call("wrong").await.unwrap();
    assert_eq!(refused.status(), 401);
    let body: Value = refused.json().await.unwrap();
    assert_eq!(body["code"], "UNAUTHORIZED");

    let first = call("s3cret").await.unwrap();
    assert_eq!(first.status(), 200);
    let first: Value = first.json().await.unwrap();
    assert_eq!(first["payload"], json!({ "user_id": HOOK_USER }));
    assert_eq!(first["receipt"]["commandId"], delivery);
    assert_eq!(first["receipt"]["state"], "succeeded");

    // A redelivery is the same command: its receipt answers.
    let again: Value = call("s3cret").await.unwrap().json().await.unwrap();
    assert_eq!(again, first);
}

#[tokio::test]
async fn an_empty_body_reaches_the_edge_as_null_and_bad_json_never_does() {
    let base = start_app(microsvc::router(hook_service())).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/{}", handlers::whoami::COMMAND);

    let empty = client
        .post(&url)
        .header(SECRET_HEADER, "s3cret")
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), 400);
    let body: Value = empty.json().await.unwrap();
    assert_eq!(body["code"], "BAD_REQUEST");

    let malformed = client
        .post(&url)
        .header(SECRET_HEADER, "s3cret")
        .body("{not json")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), 400);
}

#[tokio::test]
async fn a_command_without_an_edge_is_not_answered() {
    let base = start_app(microsvc::router(hook_service())).await;
    let client = reqwest::Client::new();

    // Registered, typed, and the caller presents a trusted-looking identity
    // and the hook secret: still not exposed.
    let response = client
        .post(format!("{base}/counter.initialize"))
        .header(SECRET_HEADER, "s3cret")
        .header("x-user-id", "user-1")
        .header("x-roles", handlers::USER_ROLE)
        .json(&json!({ "commandId": command_id(), "input": { "id": "c1" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
}

#[test]
#[should_panic(expected = "does not register")]
fn an_edge_for_an_unregistered_command_fails_at_router_build() {
    let service = Arc::new(Service::new().with_http_command("nope.v1", hook_edge));
    let _ = microsvc::router(service);
}

#[cfg(feature = "gateway-native")]
#[tokio::test]
async fn every_command_path_belongs_to_the_service_not_the_ui() {
    use distributed::gateway::{native::*, *};

    let ui = start_app(axum::Router::new().fallback(|| async { "ui" })).await;
    let service = hook_service();
    let mut routes = vec![Route::new("ui", RoutePath::prefix("/"), "ui")];
    routes.extend(service.command_gateway_routes("service"));
    let gateway = GatewayConfig {
        bindings: vec![
            Binding::new("ui", BindingKind::UiProxy { origin: ui.clone() }),
            Binding::new("service", BindingKind::Handler),
        ],
        routes,
    }
    .build()
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let app = NativeGateway::new(
        gateway,
        NativeOptions::new(&origin),
        [
            ("ui".into(), NativeBinding::UiProxy { websocket: false }),
            (
                "service".into(),
                NativeBinding::Handler(microsvc::router(service)),
            ),
        ],
        NativeAuth::anonymous(),
    )
    .unwrap()
    .router();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let client = reqwest::Client::new();

    let page = client.get(format!("{origin}/todos")).send().await.unwrap();
    assert_eq!(page.text().await.unwrap(), "ui");

    let closed = client
        .post(format!("{origin}/counter.initialize"))
        .json(&json!({ "commandId": command_id(), "input": { "id": "c1" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(closed.status(), 404);
    assert_ne!(closed.text().await.unwrap(), "ui");

    let exposed = client
        .post(format!("{origin}/{}", handlers::whoami::COMMAND))
        .header(SECRET_HEADER, "s3cret")
        .json(&json!({ "delivery": command_id() }))
        .send()
        .await
        .unwrap();
    assert_eq!(exposed.status(), 200);
}
