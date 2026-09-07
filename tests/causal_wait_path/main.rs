//! HTTP/gRPC causal wait-path and Bus send-has-no-reply.
#![cfg(all(feature = "graphql", feature = "http"))]

use std::sync::Arc;

use distributed::bus::{Bus, BusConsumer, InMemoryBus, TransportError};
use distributed::cell_host::InternalHttpSecret;
use distributed::command::{
    typed_command, CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField, Succeeded,
};
use distributed::command_dispatch::{CommandHost, HttpCommandHost, SharedCommandHost};
use distributed::graphql::VerifiedPrincipal;
use distributed::microsvc::{router, Routes, Service, ROLE_KEY, USER_ID_KEY};
use distributed::{Aggregate, AggregateBuilder, Entity, InMemoryRepository, Snapshot};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Default, Snapshot)]
struct WaitAgg {
    entity: Entity,
}

impl WaitAgg {
    fn record(&mut self, id: String) -> distributed::SourcedResult {
        self.entity.set_id(id);
        self.entity.digest_empty("wait.recorded")
    }
}

impl Aggregate for WaitAgg {
    type ReplayError = std::convert::Infallible;

    fn aggregate_type() -> &'static str {
        "causal-wait-path"
    }

    fn entity(&self) -> &Entity {
        &self.entity
    }

    fn entity_mut(&mut self) -> &mut Entity {
        &mut self.entity
    }

    fn replay_event(&mut self, _event: &distributed::EventRecord) -> Result<(), Self::ReplayError> {
        Ok(())
    }
}

#[derive(Deserialize)]
struct IdInput {
    id: String,
}

impl CommandInputType for IdInput {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "IdInput",
            vec![CommandTypeField {
                name: "id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        )
        .with_type_id(std::any::TypeId::of::<Self>())
    }
}

#[derive(Serialize)]
struct IdPayload {
    id: String,
}

impl CommandOutputType for IdPayload {
    fn command_type() -> CommandTypeDef {
        CommandTypeDef::new(
            "IdPayload",
            vec![CommandTypeField {
                name: "id".into(),
                type_name: "String".into(),
                nullable: false,
                list: false,
                item_nullable: false,
                nested: None,
            }],
        )
        .with_type_id(std::any::TypeId::of::<Self>())
    }
}

fn wait_service() -> Arc<Service> {
    let causal = Routes::new()
        .with_repo(InMemoryRepository::new().aggregate::<WaitAgg>())
        .typed_command(
            typed_command::<IdInput, Succeeded<IdPayload>>("todo.create").roles(["user"]),
        )
        .create()
        .invoke(|aggregate, input, _owner| {
            aggregate.record(input.id.clone())?;
            Ok::<_, distributed::EventRecordError>(())
        })
        .succeeded(|aggregate| IdPayload {
            id: aggregate.entity().id().to_string(),
        })
        .typed_command(
            typed_command::<IdInput, Succeeded<IdPayload>>("todo.admin_only").roles(["admin"]),
        )
        .create()
        .invoke(|aggregate, input, _owner| {
            aggregate.record(input.id.clone())?;
            Ok::<_, distributed::EventRecordError>(())
        })
        .succeeded(|aggregate| IdPayload {
            id: aggregate.entity().id().to_string(),
        });
    let ping = Routes::new().with_dependencies(()).command("ping").handle(
        |_ctx: &distributed::microsvc::Context<'_, ()>| async { Ok(json!({ "pong": true })) },
    );
    Arc::new(
        Service::new()
            .named("causal-wait-path")
            .with_http_command_routes()
            .routes(causal)
            .routes(ping),
    )
}

async fn start_http(service: Arc<Service>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(service);
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn cell_wait_path_replays_once_after_internal_failure() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::{extract::State, http::StatusCode, routing::post, Json, Router};

    async fn command(
        State(attempts): State<Arc<AtomicUsize>>,
        Json(body): Json<serde_json::Value>,
    ) -> (StatusCode, Json<serde_json::Value>) {
        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "code": "INTERNAL", "error": "transient" })),
            );
        }
        (
            StatusCode::CREATED,
            Json(json!({
                "payload": { "id": "todo-replayed" },
                "receipt": {
                    "commandId": body["commandId"],
                    "causationId": "cause-replayed",
                    "state": "succeeded",
                    "replayed": true
                },
                "events": []
            })),
        )
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .route("/todo/todo-replayed/todo.create", post(command))
        .with_state(Arc::clone(&attempts));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let host = HttpCommandHost::new_internal(
        format!("http://{addr}/todo/todo-replayed"),
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap();
    let command_id = "0190a000-0000-7000-8000-000000000106";
    let (status, body) = host
        .post_cell_wait_path(
            "todo.create",
            command_id,
            json!({ "id": "todo-replayed" }),
            &distributed::microsvc::Session::new(),
            "causal-wait-path",
            "partition-1",
        )
        .await
        .unwrap();

    assert_eq!(status, 201);
    assert_eq!(body["receipt"]["commandId"], command_id);
    assert_eq!(body["receipt"]["replayed"], true);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn http_wait_path_returns_command_id_and_receipt() {
    let base = start_http(wait_service()).await;
    let client = reqwest::Client::new();
    let command_id = "0190a000-0000-7000-8000-000000000101";
    let resp = client
        .post(format!("{base}/todo.create"))
        .header(USER_ID_KEY, "alice")
        .header(ROLE_KEY, "user")
        .json(&json!({
            "commandId": command_id,
            "input": { "id": "todo-wait-1" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["payload"], json!({ "id": "todo-wait-1" }));
    assert_eq!(body["receipt"]["commandId"], command_id);
    assert_eq!(body["receipt"]["state"], "succeeded");
    assert!(body["receipt"]["causationId"].as_str().unwrap().len() > 0);
}

#[tokio::test]
async fn http_wait_path_rejects_spoofed_body_identity() {
    let base = start_http(wait_service()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/todo.admin_only"))
        .header(USER_ID_KEY, "alice")
        .header(ROLE_KEY, "user")
        .json(&json!({
            "commandId": "0190a000-0000-7000-8000-000000000102",
            "input": { "id": "todo-admin" },
            "session_variables": { "x-roles": "admin" },
            "roles": "admin"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "{}", resp.text().await.unwrap());
}

#[tokio::test]
async fn graphql_only_http_host_wait_dispatches_to_writer() {
    let base = start_http(wait_service()).await;
    let host = HttpCommandHost::new(base).expect("valid wait-path URL");
    let mut session = distributed::microsvc::Session::new();
    session.set(USER_ID_KEY, "alice");
    session.set(ROLE_KEY, "user");
    let principal = VerifiedPrincipal::from_trusted_transport("alice");
    let command_id = "0190a000-0000-7000-8000-000000000105";
    let result = host
        .invoke(
            "todo.create",
            command_id,
            json!({ "id": "todo-gql-host" }),
            session,
            principal,
            None,
        )
        .await
        .expect("GraphQL-only host should wait-dispatch over HTTP");
    assert_eq!(result.payload(), &json!({ "id": "todo-gql-host" }));
    assert_eq!(result.command_id(), command_id);
    assert_eq!(result.state(), "succeeded");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn graphql_only_engine_wait_dispatches_to_loopback_writer() {
    use async_graphql::Request;
    use distributed::graphql::GraphqlEngine;
    use distributed::microsvc::Session;

    const PROTOCOL_TOKEN_KEY: [u8; 32] = [0x5a; 32];

    let writer = wait_service();
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let engine = GraphqlEngine::builder(pool)
        .protocol_token_key(PROTOCOL_TOKEN_KEY)
        .roles(&["user"])
        .service(writer.as_ref())
        .build()
        .expect("GraphQL schema compiles from contracts without mounting the writer");
    let mut query_session = Session::new();
    query_session.set(ROLE_KEY, "user");
    let query = engine
        .execute(&query_session, Request::new("{ __typename }"))
        .await;
    assert!(
        query.errors.is_empty(),
        "SQL/local GraphQL query: {query:?}"
    );

    let base = start_http(Arc::clone(&writer)).await;
    let host: SharedCommandHost =
        Arc::new(HttpCommandHost::new(base).expect("valid wait-path URL"));
    let mut session = Session::new();
    session.set(USER_ID_KEY, "alice");
    session.set(ROLE_KEY, "user");
    let principal = VerifiedPrincipal::from_trusted_transport("alice");
    let command_id = "0190a000-0000-7000-8000-000000000106";
    let mutation = engine
        .execute(
            &session,
            Request::new(format!(
                "mutation {{ todo_create(commandId: \"{command_id}\", input: {{ id: \"todo-gql-only\" }}) {{ id }} }}"
            ))
            .data(Arc::clone(&host))
            .data(principal),
        )
        .await;
    assert!(
        mutation.errors.is_empty(),
        "GraphQL-only wait-dispatch: {mutation:?}"
    );
    let data = mutation.data.into_json().unwrap();
    assert_eq!(data["todo_create"]["id"], "todo-gql-only");
}

#[tokio::test]
async fn bus_send_has_no_reply_value() {
    let bus = InMemoryBus::new();
    let result: Result<(), TransportError> = bus.send("ping", b"{}".to_vec()).await;
    result.expect("send is fire-and-forget");
}

#[tokio::test]
async fn same_host_listen_ping_and_http_wait_path() {
    let bus = InMemoryBus::new();
    let service = Arc::new(
        Service::new()
            .named("causal-wait-path")
            .with_http_command_routes()
            .routes(
                Routes::new()
                    .with_repo(InMemoryRepository::new().aggregate::<WaitAgg>())
                    .typed_command(
                        typed_command::<IdInput, Succeeded<IdPayload>>("todo.create")
                            .roles(["user"]),
                    )
                    .create()
                    .invoke(|aggregate, input, _owner| {
                        aggregate.record(input.id.clone())?;
                        Ok::<_, distributed::EventRecordError>(())
                    })
                    .succeeded(|aggregate| IdPayload {
                        id: aggregate.entity().id().to_string(),
                    }),
            )
            .routes(Routes::new().with_dependencies(()).command("ping").handle(
                |_ctx: &distributed::microsvc::Context<'_, ()>| async {
                    Ok(json!({ "pong": true }))
                },
            ))
            .with_bus(bus.clone()),
    );
    {
        let bus = bus.clone();
        let service = Arc::clone(&service);
        tokio::spawn(async move {
            let _ = bus
                .listen(service, distributed::bus::RunOptions::default())
                .await;
        });
    }
    bus.send("ping", b"{}".to_vec()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let base = start_http(Arc::clone(&service)).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/todo.create"))
        .header(USER_ID_KEY, "alice")
        .header(ROLE_KEY, "user")
        .json(&json!({
            "commandId": "0190a000-0000-7000-8000-000000000103",
            "input": { "id": "todo-host-1" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{}", resp.text().await.unwrap());
}

#[cfg(feature = "grpc")]
#[tokio::test]
async fn grpc_wait_path_returns_command_id_and_receipt() {
    use distributed::microsvc::grpc::{CommandServiceClient, GrpcRequest};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    let service = wait_service();
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
    let mut client = CommandServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let command_id = "0190a000-0000-7000-8000-000000000104";
    let mut request = tonic::Request::new(GrpcRequest {
        command: "todo.create".into(),
        input: json!({
            "commandId": command_id,
            "input": { "id": "todo-grpc-1" }
        })
        .to_string(),
        session_variables: Default::default(),
    });
    request
        .metadata_mut()
        .insert(USER_ID_KEY, "alice".parse().unwrap());
    request
        .metadata_mut()
        .insert(ROLE_KEY, "user".parse().unwrap());
    let resp = client.dispatch(request).await.unwrap().into_inner();
    assert_eq!(resp.status, 200, "{}", resp.body);
    let body: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(body["payload"], json!({ "id": "todo-grpc-1" }));
    assert_eq!(body["receipt"]["commandId"], command_id);
    assert_eq!(body["receipt"]["state"], "succeeded");
}
