//! HTTP/gRPC causal wait-path and Bus send-has-no-reply.
#![cfg(all(feature = "graphql", feature = "http"))]

use std::sync::Arc;

use axum::http::StatusCode;
use distributed::bus::{Bus, BusConsumer, InMemoryBus, TransportError};
use distributed::cell_host::{
    AggregateCell, CellCommandIdentity, CelldCommandHost, CelldRoute, InternalHttpSecret,
    CELL_CAUSATION_ID_HEADER, CELL_INTERNAL_SECRET_HEADER, CELL_PRINCIPAL_PARTITION_HEADER,
    CELL_SERVICE_ID_HEADER,
};
use distributed::command::{
    typed_command, CommandInputType, CommandOutputType, CommandTypeDef, CommandTypeField,
    PreparedCommand, Succeeded,
};
use distributed::command_dispatch::{
    CellRequestContext, CommandHost, HttpCommandHost, SharedCommandHost, TrustedRequestMetadata,
};
use distributed::graphql::VerifiedPrincipal;
use distributed::microsvc::{
    router, CausalCommandContext, HandlerError, PortableCommand, Routes, Service, Session,
    ROLE_KEY, USER_ID_KEY,
};
use distributed::{Aggregate, AggregateBuilder, Entity, InMemoryRepository, Snapshot};
#[cfg(feature = "sqlite")]
use distributed::{AggregateRepository, SqliteRepository};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;

const TRUSTED_CAUSATION_ID: &str = "0190a000-0000-7000-8000-000000000999";

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

fn wait_service_with_repo(repo: distributed::InMemoryRepository) -> Arc<Service> {
    let causal = Routes::new()
        .with_repo(repo.aggregate::<WaitAgg>())
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

fn wait_service() -> Arc<Service> {
    wait_service_with_repo(InMemoryRepository::new())
}

async fn handle_trusted_create(
    ctx: &CausalCommandContext<'_, WaitAgg>,
    input: IdInput,
) -> Result<PreparedCommand<Succeeded<IdPayload>>, HandlerError> {
    let trusted = [
        ("x-producer", "canonical-fact"),
        (CELL_SERVICE_ID_HEADER, "generic-writer"),
        (CELL_PRINCIPAL_PARTITION_HEADER, "generic-partition"),
        (CELL_CAUSATION_ID_HEADER, TRUSTED_CAUSATION_ID),
    ]
    .into_iter()
    .all(|(key, expected)| ctx.claim(key) == Some(expected));
    if !trusted {
        return Err(HandlerError::Unauthorized(
            "trusted producer metadata missing".into(),
        ));
    }
    let repo = ctx.repo();
    if repo.get(&input.id).await?.is_some() {
        return Err(HandlerError::Rejected(format!(
            "cell item {} already exists",
            input.id
        )));
    }
    let mut item = repo.create();
    item.record(input.id.clone())
        .map_err(|error| HandlerError::Rejected(error.to_string()))?;
    repo.commit(item)?.succeeded(IdPayload { id: input.id })
}

struct TrustedCreate;

impl<D> PortableCommand<D> for TrustedCreate
where
    D: distributed::microsvc::CausalRouteDependencies<Aggregate = WaitAgg> + Send + Sync + 'static,
{
    fn install(self, routes: Routes<D>) -> Routes<D> {
        routes
            .typed_command(typed_command::<IdInput, Succeeded<IdPayload>>(
                "generic.grant",
            ))
            .guarded(
                |_ctx: &CausalCommandContext<'_, WaitAgg>| true,
                handle_trusted_create,
            )
    }
}

#[cfg(feature = "sqlite")]
fn sqlite_wait_service(repository: SqliteRepository) -> Arc<Service> {
    let causal = Routes::new()
        .with_repo(AggregateRepository::<_, WaitAgg>::new(repository))
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
        });
    Arc::new(
        Service::new()
            .named("causal-wait-path")
            .with_http_command_routes()
            .routes(causal),
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

#[derive(Clone)]
struct TypedCellBridgeState {
    cell: Arc<AggregateCell<WaitAgg>>,
    secret: InternalHttpSecret,
}

fn typed_cell_bridge_error(
    status: StatusCode,
    code: impl Into<String>,
    message: impl Into<String>,
) -> (StatusCode, axum::Json<Value>) {
    (
        status,
        axum::Json(json!({
            "code": code.into(),
            "error": message.into(),
        })),
    )
}

async fn typed_cell_bridge(
    axum::extract::State(state): axum::extract::State<TypedCellBridgeState>,
    headers: axum::http::HeaderMap,
    axum::Json(body): axum::Json<Value>,
) -> (StatusCode, axum::Json<Value>) {
    let Some(secret) = headers
        .get(CELL_INTERNAL_SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return typed_cell_bridge_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing internal cell secret",
        );
    };
    if !state.secret.matches(secret) {
        return typed_cell_bridge_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "invalid internal cell secret",
        );
    }

    let required_header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let Some(service_id) = required_header(CELL_SERVICE_ID_HEADER) else {
        return typed_cell_bridge_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing cell service identity",
        );
    };
    let Some(principal_partition) = required_header(CELL_PRINCIPAL_PARTITION_HEADER) else {
        return typed_cell_bridge_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing cell principal partition",
        );
    };
    let Some(causation_id) = required_header(CELL_CAUSATION_ID_HEADER) else {
        return typed_cell_bridge_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "missing cell causation identity",
        );
    };
    let Some(command_id) = body.get("commandId").and_then(Value::as_str) else {
        return typed_cell_bridge_error(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            "missing commandId",
        );
    };
    let input = body.get("input").cloned().unwrap_or(Value::Null);
    let identity = match CellCommandIdentity::new(service_id, principal_partition, command_id)
        .and_then(|identity| identity.with_causation_id(causation_id))
    {
        Ok(identity) => identity,
        Err(error) => {
            return typed_cell_bridge_error(
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_REQUEST),
                error.code(),
                error.client_message(),
            )
        }
    };

    // The bridge only constructs a Session after authenticating the internal
    // host boundary. Public callers cannot turn arbitrary request headers into
    // trusted producer claims because they cannot pass the secret check.
    let mut session = Session::new();
    for (name, value) in &headers {
        if let Ok(value) = value.to_str() {
            session.set(name.as_str(), value);
        }
    }

    match state
        .cell
        .dispatch_idempotent("generic.grant", &identity, input, session)
        .await
    {
        Ok(result) => (
            StatusCode::OK,
            axum::Json(json!({
                "payload": result.payload(),
                "receipt": {
                    "commandId": result.command_id(),
                    "causationId": result.causation_id(),
                    "state": result.state(),
                    "replayed": result.replayed(),
                },
                "events": result.projection_events(),
            })),
        ),
        Err(error) => typed_cell_bridge_error(
            StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            error.code(),
            error.client_message(),
        ),
    }
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
async fn trusted_native_metadata_reaches_cell_without_forwarding_session_headers() {
    use axum::{
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::post,
        Json, Router,
    };
    use std::sync::Mutex;

    async fn command(
        State(seen): State<Arc<Mutex<Vec<Option<String>>>>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        seen.lock().unwrap().push(
            headers
                .get("x-producer")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
        (
            StatusCode::CREATED,
            Json(json!({
                "payload": { "id": body["input"]["id"] },
                "receipt": {
                    "commandId": body["commandId"],
                    "causationId": "cause-trusted",
                    "state": "succeeded",
                    "replayed": false
                },
                "events": []
            })),
        )
    }

    let seen = Arc::new(Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(post(command))
        .with_state(Arc::clone(&seen));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let host = HttpCommandHost::new_internal(
        format!("http://{addr}"),
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap();
    let mut session = distributed::microsvc::Session::new();
    session.set(USER_ID_KEY, "native-user");
    session.set(ROLE_KEY, "system");
    session.set("x-producer", "forged-public-header");
    let metadata =
        TrustedRequestMetadata::try_from_pairs([("x-producer", "canonical-fact")]).unwrap();
    let context = CellRequestContext::new("generic-writer", "generic-partition")
        .with_causation_id("canonical-causation")
        .with_trusted_metadata(metadata);

    host.post_cell_wait_path_with_context(
        "generic.grant",
        "0190a000-0000-7000-8000-000000000120",
        json!({ "id": "generic-grant-1" }),
        &session,
        &context,
    )
    .await
    .unwrap();
    host.post_cell_wait_path(
        "generic.grant",
        "0190a000-0000-7000-8000-000000000121",
        json!({ "id": "generic-grant-2" }),
        &session,
        "generic-writer",
        "generic-partition",
    )
    .await
    .unwrap();

    assert_eq!(
        *seen.lock().unwrap(),
        vec![Some("canonical-fact".into()), None],
        "only the explicit trusted context may carry custom producer metadata"
    );
}

#[tokio::test]
async fn trusted_native_metadata_reaches_a_real_aggregate_cell() {
    use axum::{routing::post, Router};

    let secret = InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap();
    let cell = Arc::new(
        AggregateCell::<WaitAgg>::new("typed-cell-grant")
            .unwrap()
            .mount(TrustedCreate),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(post(typed_cell_bridge))
        .with_state(TypedCellBridgeState {
            cell: Arc::clone(&cell),
            secret: secret.clone(),
        });
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let base = format!("http://{addr}");
    let host = HttpCommandHost::new_internal(base.clone(), secret.clone()).unwrap();
    let mut session = distributed::microsvc::Session::new();
    session.set(USER_ID_KEY, "native-user");
    session.set(ROLE_KEY, "system");
    // This value models a public request header. The explicit native context
    // below is the only trusted producer source for the cell command.
    session.set("x-producer", "forged-public-header");
    let metadata =
        TrustedRequestMetadata::try_from_pairs([("x-producer", "canonical-fact")]).unwrap();
    let context = CellRequestContext::new("generic-writer", "generic-partition")
        .with_causation_id(TRUSTED_CAUSATION_ID)
        .with_trusted_metadata(metadata);

    let (status, body) = host
        .post_cell_wait_path_with_context(
            "generic.grant",
            "0190a000-0000-7000-8000-000000000122",
            json!({ "id": "typed-cell-grant" }),
            &session,
            &context,
        )
        .await
        .expect("typed cell command should receive the trusted context");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["payload"], json!({ "id": "typed-cell-grant" }));
    assert_eq!(body["receipt"]["state"], "succeeded");
    assert_eq!(body["receipt"]["causationId"], TRUSTED_CAUSATION_ID);
    assert_eq!(body["receipt"]["replayed"], false);
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1,
        "valid trusted cell dispatch must commit one aggregate event"
    );

    let (status, body) = host
        .post_cell_wait_path_with_context(
            "generic.grant",
            "0190a000-0000-7000-8000-000000000122",
            json!({ "id": "typed-cell-grant" }),
            &session,
            &context,
        )
        .await
        .expect("same cell command identity should replay its durable result");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["receipt"]["replayed"], true);
    assert_eq!(body["receipt"]["causationId"], TRUSTED_CAUSATION_ID);
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1,
        "replay must not append another aggregate event"
    );

    let (status, body) = host
        .post_cell_wait_path_with_context(
            "generic.grant",
            "0190a000-0000-7000-8000-000000000122",
            json!({ "id": "different-input" }),
            &session,
            &context,
        )
        .await
        .expect("changed input should return a durable command-id conflict");
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["code"], "COMMAND_ID_REUSE");
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1,
        "a reused identity with a changed body must not append an event"
    );

    let client = reqwest::Client::new();
    let missing_secret = client
        .post(format!("{base}/generic.grant"))
        .header(CELL_SERVICE_ID_HEADER, "generic-writer")
        .header(CELL_PRINCIPAL_PARTITION_HEADER, "generic-partition")
        .header(CELL_CAUSATION_ID_HEADER, TRUSTED_CAUSATION_ID)
        .header("x-producer", "canonical-fact")
        .json(&json!({
            "commandId": "0190a000-0000-7000-8000-000000000124",
            "input": { "id": "missing-secret" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing_secret.status(), StatusCode::UNAUTHORIZED);

    let wrong_secret = client
        .post(format!("{base}/generic.grant"))
        .header(CELL_INTERNAL_SECRET_HEADER, "wrong-test-only-secret")
        .header(CELL_SERVICE_ID_HEADER, "generic-writer")
        .header(CELL_PRINCIPAL_PARTITION_HEADER, "generic-partition")
        .header(CELL_CAUSATION_ID_HEADER, TRUSTED_CAUSATION_ID)
        .header("x-producer", "canonical-fact")
        .json(&json!({
            "commandId": "0190a000-0000-7000-8000-000000000125",
            "input": { "id": "wrong-secret" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_secret.status(), StatusCode::UNAUTHORIZED);

    let (status, body) = host
        .post_cell_wait_path_with_causation(
            "generic.grant",
            "0190a000-0000-7000-8000-000000000126",
            json!({ "id": "missing-producer" }),
            &session,
            "generic-writer",
            "generic-partition",
            Some(TRUSTED_CAUSATION_ID),
        )
        .await
        .expect("typed guard rejection should be returned by the cell bridge");
    assert_eq!(status, StatusCode::UNAUTHORIZED.as_u16(), "{body}");
    assert!(body["error"]
        .as_str()
        .is_some_and(|message| message.contains("trusted producer metadata")));
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1,
        "unauthenticated provenance must not append an aggregate event"
    );
}

#[tokio::test]
async fn authenticated_transport_recovery_preserves_no_effects_then_replays() {
    use axum::{routing::post, Router};

    let secret = InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap();
    let cell = Arc::new(
        AggregateCell::<WaitAgg>::new("recovery-cell")
            .unwrap()
            .mount(TrustedCreate),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(post(typed_cell_bridge))
        .with_state(TypedCellBridgeState {
            cell: Arc::clone(&cell),
            secret: secret.clone(),
        });
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let base = format!("http://{addr}");
    let host = HttpCommandHost::new_internal(base, secret).unwrap();
    let mut session = distributed::microsvc::Session::new();
    session.set(USER_ID_KEY, "native-user");
    session.set(ROLE_KEY, "system");

    // The original command is authenticated at the HTTP boundary but lacks
    // the producer claim required by the destination cell. This fixture only
    // proves that the rejected request produced no aggregate event; a caller
    // must inspect its durable receipt/error classification before recovery.
    let (status, body) = host
        .post_cell_wait_path_with_causation(
            "generic.grant",
            "0190a000-0000-7000-8000-000000000220",
            json!({ "id": "recovery-cell" }),
            &session,
            "generic-writer",
            "generic-partition",
            Some(TRUSTED_CAUSATION_ID),
        )
        .await
        .unwrap();
    assert_eq!(status, StatusCode::UNAUTHORIZED.as_u16(), "{body}");
    assert!(body["error"]
        .as_str()
        .is_some_and(|message| message.contains("trusted producer metadata")));
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        0
    );

    let metadata =
        TrustedRequestMetadata::try_from_pairs([("x-producer", "canonical-fact")]).unwrap();
    let context = CellRequestContext::new("generic-writer", "generic-partition")
        .with_causation_id(TRUSTED_CAUSATION_ID)
        .with_trusted_metadata(metadata);
    let recovery_command_id = "0190a000-0000-7000-8000-000000000221";

    let (status, body) = host
        .post_cell_wait_path_with_context(
            "generic.grant",
            recovery_command_id,
            json!({ "id": "recovery-cell" }),
            &session,
            &context,
        )
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK.as_u16(), "{body}");
    assert_eq!(body["receipt"]["replayed"], false);
    assert_eq!(body["receipt"]["causationId"], TRUSTED_CAUSATION_ID);
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1
    );

    let (status, body) = host
        .post_cell_wait_path_with_context(
            "generic.grant",
            recovery_command_id,
            json!({ "id": "recovery-cell" }),
            &session,
            &context,
        )
        .await
        .unwrap();
    assert_eq!(status, StatusCode::OK.as_u16(), "{body}");
    assert_eq!(body["receipt"]["replayed"], true);
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1
    );

    let (status, body) = host
        .post_cell_wait_path_with_context(
            "generic.grant",
            recovery_command_id,
            json!({ "id": "changed-recovery-body" }),
            &session,
            &context,
        )
        .await
        .unwrap();
    assert_eq!(status, StatusCode::CONFLICT.as_u16(), "{body}");
    assert_eq!(body["code"], "COMMAND_ID_REUSE");
    assert_eq!(
        cell.durable_events()
            .unwrap()
            .iter()
            .map(|stream| stream.events.len())
            .sum::<usize>(),
        1
    );
}

#[tokio::test]
async fn celld_host_completes_remote_commit_once_and_rejects_changed_shard() {
    use axum::{extract::State, http::HeaderMap, http::StatusCode, routing::post, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn command(
        State(calls): State<Arc<AtomicUsize>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        calls.fetch_add(1, Ordering::SeqCst);
        let causation_id = headers
            .get(CELL_CAUSATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("gateway must bind the cell request to its reserved causation")
            .to_string();
        (
            StatusCode::CREATED,
            Json(json!({
                "payload": { "id": body["input"]["id"] },
                "receipt": {
                    "commandId": body["commandId"],
                    "causationId": causation_id,
                    "state": "succeeded",
                    "replayed": false
                },
                "events": []
            })),
        )
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(post(command))
        .with_state(Arc::clone(&calls));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let repo = InMemoryRepository::new();
    let service = wait_service_with_repo(repo.clone());
    let host = CelldCommandHost::new(
        format!("http://{addr}"),
        Arc::clone(&service),
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap()
    .route(CelldRoute::new(
        &["todo.create"],
        "todo",
        |input| input.get("id").and_then(Value::as_str).map(str::to_owned),
        |_command, _input, remote, _session| remote.clone(),
    ));
    let mut session = distributed::microsvc::Session::new();
    session.set(USER_ID_KEY, "alice");
    session.set(ROLE_KEY, "user");
    let principal = VerifiedPrincipal::from_trusted_transport("alice");
    let command_id = "0190a000-0000-7000-8000-000000000109";

    let first = host
        .invoke(
            "todo.create",
            command_id,
            json!({ "id": "todo-cell-once" }),
            session.clone(),
            principal.clone(),
            None,
        )
        .await
        .expect("remote cell commit should complete the gateway ledger");
    assert_eq!(first.payload(), &json!({ "id": "todo-cell-once" }));
    assert_eq!(first.state(), "succeeded");

    drop(host);
    drop(service);
    let restarted_service = wait_service_with_repo(repo);
    let restarted_host = CelldCommandHost::new(
        format!("http://{addr}"),
        Arc::clone(&restarted_service),
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap()
    .route(CelldRoute::new(
        &["todo.create"],
        "todo",
        |input| input.get("id").and_then(Value::as_str).map(str::to_owned),
        |_command, _input, remote, _session| remote.clone(),
    ));
    let replay = restarted_host
        .invoke(
            "todo.create",
            command_id,
            json!({ "id": "todo-cell-once" }),
            session.clone(),
            principal.clone(),
            None,
        )
        .await
        .expect("completed external receipt should replay durably");
    assert_eq!(replay.state(), "succeeded");
    assert_eq!(replay.payload(), first.payload());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let conflict = restarted_host
        .invoke(
            "todo.create",
            command_id,
            json!({ "id": "todo-other-cell" }),
            session,
            principal,
            None,
        )
        .await
        .expect_err("same command ID cannot move to another cell or input");
    assert!(matches!(
        conflict,
        distributed::microsvc::CausalDispatchError::CommandIdReuse
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let different_binding_host = CelldCommandHost::new(
        format!("http://{addr}"),
        Arc::clone(&restarted_service),
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap()
    .route(CelldRoute::new(
        &["todo.create"],
        "todo",
        |_input| Some("todo-different-cell".into()),
        |_command, _input, remote, _session| remote.clone(),
    ));
    let mut binding_session = distributed::microsvc::Session::new();
    binding_session.set(USER_ID_KEY, "alice");
    binding_session.set(ROLE_KEY, "user");
    let binding_conflict = different_binding_host
        .invoke(
            "todo.create",
            command_id,
            json!({ "id": "todo-cell-once" }),
            binding_session,
            VerifiedPrincipal::from_trusted_transport("alice"),
            None,
        )
        .await
        .expect_err("the immutable route binding must fence a changed shard");
    assert!(matches!(
        binding_conflict,
        distributed::microsvc::CausalDispatchError::CommandIdReuse
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn celld_host_reclaims_an_ambiguous_receipt_with_the_same_causation() {
    use axum::{extract::State, http::HeaderMap, http::StatusCode, routing::post, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    async fn command(
        State(state): State<Arc<(AtomicUsize, Mutex<Option<String>>) >>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let causation_id = headers
            .get(CELL_CAUSATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("gateway must bind the cell request to its reserved causation")
            .to_string();
        let call = state.0.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            *state.1.lock().unwrap() = Some(causation_id);
            return (StatusCode::OK, Json(json!({ "not": "a receipt" })));
        }
        assert_eq!(
            state.1.lock().unwrap().as_deref(),
            Some(causation_id.as_str()),
            "reclaim must reuse the cell's original causation identity"
        );
        (
            StatusCode::CREATED,
            Json(json!({
                "payload": { "id": body["input"]["id"] },
                "receipt": {
                    "commandId": body["commandId"],
                    "causationId": causation_id,
                    "state": "succeeded",
                    "replayed": true
                },
                "events": []
            })),
        )
    }

    let state = Arc::new((AtomicUsize::new(0), Mutex::new(None)));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(post(command))
        .with_state(Arc::clone(&state));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let service = wait_service();
    let host = CelldCommandHost::new(
        format!("http://{addr}"),
        service,
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap()
    .route(CelldRoute::new(
        &["todo.create"],
        "todo",
        |input| input.get("id").and_then(Value::as_str).map(str::to_owned),
        |_command, _input, remote, _session| remote.clone(),
    ));
    let mut session = distributed::microsvc::Session::new();
    session.set(USER_ID_KEY, "alice");
    session.set(ROLE_KEY, "user");
    let principal = VerifiedPrincipal::from_trusted_transport("alice");
    let command_id = "0190a000-0000-7000-8000-000000000110";

    let first = host
        .invoke(
            "todo.create",
            command_id,
            json!({ "id": "todo-ambiguous" }),
            session.clone(),
            principal.clone(),
            None,
        )
        .await
        .expect_err("an undecodable remote response must remain retryable");
    assert!(matches!(
        first,
        distributed::microsvc::CausalDispatchError::Internal(_)
    ));

    let retry = host
        .invoke(
            "todo.create",
            command_id,
            json!({ "id": "todo-ambiguous" }),
            session,
            principal,
            None,
        )
        .await
        .expect("retry should reclaim the durable reservation");
    assert_eq!(retry.state(), "succeeded");
    assert_eq!(state.0.load(Ordering::SeqCst), 2);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn celld_host_replays_external_completion_after_sqlite_reopen() {
    use axum::{extract::State, http::HeaderMap, http::StatusCode, routing::post, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn command(
        State(calls): State<Arc<AtomicUsize>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        calls.fetch_add(1, Ordering::SeqCst);
        let causation_id = headers
            .get(CELL_CAUSATION_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .expect("gateway must bind the cell request to its reserved causation");
        (
            StatusCode::CREATED,
            Json(json!({
                "payload": { "id": body["input"]["id"] },
                "receipt": {
                    "commandId": body["commandId"],
                    "causationId": causation_id,
                    "state": "succeeded",
                    "replayed": false
                },
                "events": []
            })),
        )
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(post(command))
        .with_state(Arc::clone(&calls));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let database_path = std::env::temp_dir().join(format!(
        "distributed-celld-host-{}.sqlite",
        uuid::Uuid::now_v7()
    ));
    let database_url = format!("sqlite://{}?mode=rwc", database_path.display());
    let repository = SqliteRepository::connect_and_migrate(&database_url)
        .await
        .expect("initial gateway ledger migration");
    let service = sqlite_wait_service(repository.clone());
    let host = CelldCommandHost::new(
        format!("http://{addr}"),
        Arc::clone(&service),
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap()
    .route(CelldRoute::new(
        &["todo.create"],
        "todo",
        |input| input.get("id").and_then(Value::as_str).map(str::to_owned),
        |_command, _input, remote, _session| remote.clone(),
    ));
    let mut session = distributed::microsvc::Session::new();
    session.set(USER_ID_KEY, "alice");
    session.set(ROLE_KEY, "user");
    let principal = VerifiedPrincipal::from_trusted_transport("alice");
    let command_id = "0190a000-0000-7000-8000-000000000111";
    let input = json!({ "id": "todo-sqlite-reopen" });

    host.invoke(
        "todo.create",
        command_id,
        input.clone(),
        session.clone(),
        principal.clone(),
        None,
    )
    .await
    .expect("cell completion should be durable before response");
    drop(host);
    drop(service);
    drop(repository);

    let reopened_repository = SqliteRepository::connect_and_migrate(&database_url)
        .await
        .expect("reopen gateway ledger");
    let reopened_service = sqlite_wait_service(reopened_repository);
    let reopened_host = CelldCommandHost::new(
        format!("http://{addr}"),
        Arc::clone(&reopened_service),
        InternalHttpSecret::new("test-only-internal-secret-32-bytes").unwrap(),
    )
    .unwrap()
    .route(CelldRoute::new(
        &["todo.create"],
        "todo",
        |input| input.get("id").and_then(Value::as_str).map(str::to_owned),
        |_command, _input, remote, _session| remote.clone(),
    ));
    let replay = reopened_host
        .invoke(
            "todo.create",
            command_id,
            input,
            session,
            principal,
            None,
        )
        .await
        .expect("reopened gateway should replay the durable cell receipt");
    assert_eq!(replay.state(), "succeeded");
    assert_eq!(replay.payload(), &json!({ "id": "todo-sqlite-reopen" }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let _ = std::fs::remove_file(database_path);
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
