//! gRPC transport integration tests.
//!
//! Starts a tonic gRPC server and exercises it with the generated client.

use std::sync::Arc;

use distributed::microsvc::grpc::{
    CommandServiceClient, GrpcRequest, GrpcServeError, HealthRequest,
};
use distributed::microsvc::{Routes, Service};
use distributed::{AggregateBuilder, InMemoryRepository, Queueable};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use crate::commands::command_id;
use crate::handlers;
use crate::models::counter::Counter;

fn counter_service() -> Arc<Service> {
    Arc::new(
        Service::new().named("counters").routes(
            Routes::new()
                .with_repo(InMemoryRepository::new().queued().aggregate::<Counter>())
                .typed_command(handlers::counter_create::command())
                .handle(handlers::counter_create::handle)
                .typed_command(handlers::counter_increment::command())
                .handle(handlers::counter_increment::handle)
                .typed_command(handlers::whoami::command())
                .handle(handlers::whoami::handle),
        ),
    )
}

/// The `{ commandId, input }` body every command request carries.
fn envelope(input: serde_json::Value) -> String {
    json!({ "commandId": command_id(), "input": input }).to_string()
}

/// A counter command as the signed-in user a trusted proxy would inject
/// into the transport metadata.
fn counter_request(command: &str, input: serde_json::Value) -> tonic::Request<GrpcRequest> {
    let mut request = tonic::Request::new(GrpcRequest {
        command: command.into(),
        input: envelope(input),
        session_variables: Default::default(),
    });
    request
        .metadata_mut()
        .insert("x-user-id", "user-1".parse().unwrap());
    request
        .metadata_mut()
        .insert("x-roles", handlers::USER_ROLE.parse().unwrap());
    request
}

/// Bind to port 0, spawn the gRPC server, and return a connected client.
async fn start_server(service: Arc<Service>) -> CommandServiceClient<tonic::transport::Channel> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let grpc_svc = distributed::microsvc::grpc_server(service);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(grpc_svc)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Connect client
    let endpoint = format!("http://{addr}");
    CommandServiceClient::connect(endpoint).await.unwrap()
}

#[tokio::test]
async fn health_check() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let resp = client.health(HealthRequest {}).await.unwrap().into_inner();

    assert!(resp.ok);
    assert!(resp.commands.iter().any(|c| c == "counter.initialize"));
    assert!(resp.commands.iter().any(|c| c == "counter.increment"));
}

#[tokio::test]
async fn serve_grpc_returns_error_for_invalid_address() {
    let err = distributed::microsvc::serve_grpc(counter_service(), "not an address")
        .await
        .expect_err("invalid gRPC bind address should return an error");

    match err {
        GrpcServeError::InvalidAddress { addr, .. } => {
            assert_eq!(addr, "not an address");
        }
        other => panic!("unexpected gRPC serve error: {other:?}"),
    }
}

#[tokio::test]
async fn create_counter() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let resp = client
        .dispatch(counter_request("counter.initialize", json!({ "id": "c1" })))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["payload"], json!({ "id": "c1" }));
}

#[tokio::test]
async fn create_and_increment_counter() {
    let service = counter_service();
    let mut client = start_server(service).await;

    // Create
    let resp = client
        .dispatch(counter_request("counter.initialize", json!({ "id": "c1" })))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, 200);

    // Increment
    let resp = client
        .dispatch(counter_request(
            "counter.increment",
            json!({ "id": "c1", "amount": 5 }),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["payload"], json!({ "id": "c1", "value": 5 }));

    // Increment again
    let resp = client
        .dispatch(counter_request(
            "counter.increment",
            json!({ "id": "c1", "amount": 3 }),
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["payload"], json!({ "id": "c1", "value": 8 }));
}

#[tokio::test]
async fn unknown_command_is_a_bad_request() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let resp = client
        .dispatch(counter_request("nonexistent", json!({})))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, 400);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["code"], "BAD_REQUEST");
    assert_eq!(body["error"], "unknown typed command");
}

/// Every command is ledgered: input that is not the `{ commandId, input }`
/// envelope names no command id, so it is refused rather than run.
#[tokio::test]
async fn input_without_a_command_id_is_a_bad_request() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let mut request = counter_request("counter.initialize", json!({}));
    request.get_mut().input = json!({ "id": "c1" }).to_string();
    let resp = client.dispatch(request).await.unwrap().into_inner();

    assert_eq!(resp.status, 400);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["code"], "BAD_REQUEST");
    assert!(
        body["error"].as_str().unwrap().contains("commandId"),
        "{body}"
    );
}

#[tokio::test]
async fn metadata_flows_to_session() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let mut request = tonic::Request::new(GrpcRequest {
        command: "session.identify".into(),
        input: envelope(json!({})),
        session_variables: Default::default(),
    });
    request
        .metadata_mut()
        .insert("x-user-id", "user-42".parse().unwrap());

    let resp = client.dispatch(request).await.unwrap().into_inner();

    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["payload"], json!({ "user_id": "user-42" }));
}

/// Security: a client must not be able to spoof identity via the payload.
/// Transport metadata is injected by a trusted proxy and MUST win over the
/// client-controlled `session_variables`. Here the payload claims to be
/// `attacker`, but the proxy-injected metadata says `trusted-user` — the
/// handler must see `trusted-user`.
#[tokio::test]
async fn grpc_payload_session_variables_do_not_override_metadata() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let mut payload_vars = std::collections::HashMap::new();
    payload_vars.insert("x-user-id".to_string(), "attacker".to_string());

    let mut request = tonic::Request::new(GrpcRequest {
        command: "session.identify".into(),
        input: envelope(json!({})),
        session_variables: payload_vars,
    });
    request
        .metadata_mut()
        .insert("x-user-id", "trusted-user".parse().unwrap());

    let resp = client.dispatch(request).await.unwrap().into_inner();

    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    // Trusted metadata wins; the payload-supplied identity is ignored.
    assert_eq!(body["payload"], json!({ "user_id": "trusted-user" }));
}

/// Payload-only session variables (no colliding metadata) still flow through,
/// supporting gateway action/webhook shapes where verified claims arrive in
/// the payload and no transport metadata is injected.
#[tokio::test]
async fn grpc_payload_session_variables_apply_when_metadata_absent() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let mut payload_vars = std::collections::HashMap::new();
    payload_vars.insert("x-user-id".to_string(), "user-from-payload".to_string());

    let resp = client
        .dispatch(GrpcRequest {
            command: "session.identify".into(),
            input: envelope(json!({})),
            session_variables: payload_vars,
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, 200);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["payload"], json!({ "user_id": "user-from-payload" }));
}

#[tokio::test]
async fn missing_session_returns_401_status() {
    let service = counter_service();
    let mut client = start_server(service).await;

    let resp = client
        .dispatch(GrpcRequest {
            command: "session.identify".into(),
            input: envelope(json!({})),
            session_variables: Default::default(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.status, 401);
}
