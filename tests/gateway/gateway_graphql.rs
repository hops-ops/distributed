use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema, Subscription};
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::{
    extract::ws::WebSocketUpgrade,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use distributed::gateway::{native::*, *};
use distributed::graphql::IdentityConfig;
use distributed::graphql::{read, GraphqlEngine, ModelPermissions};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

struct Server {
    origin: String,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn serve(router: Router) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    Server {
        origin,
        task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    }
}
fn mounted(
    binding: GraphqlBinding,
    executor: GraphqlExecutor,
    caps: GraphqlCapabilities,
    extensions: Vec<String>,
) -> Router {
    mounted_with_options(
        binding,
        executor,
        caps,
        extensions,
        NativeOptions::new("https://public.example.invalid"),
    )
}
fn mounted_with_options(
    binding: GraphqlBinding,
    executor: GraphqlExecutor,
    caps: GraphqlCapabilities,
    extensions: Vec<String>,
    options: NativeOptions,
) -> Router {
    let gateway = GatewayConfig {
        bindings: vec![Binding::new(
            "graphql",
            BindingKind::Graphql {
                executor,
                capabilities: caps,
                delivery: DeliveryCapabilities::default(),
                schema_extensions: extensions,
            },
        )],
        routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "graphql")],
    }
    .build()
    .unwrap();
    NativeGateway::new(
        gateway,
        options,
        [("graphql".into(), NativeBinding::Graphql(binding))],
        NativeAuth::anonymous(),
    )
    .unwrap()
    .router()
}
struct CustomQuery;
#[Object]
impl CustomQuery {
    async fn extension_value(&self, input: String) -> String {
        input
    }
}
fn custom_router(filter: distributed::graphql::http::GraphqlOperationFilter) -> Router {
    let schema = Schema::build(CustomQuery, EmptyMutation, EmptySubscription).finish();
    Router::new().route(
        "/graphql",
        post(move |request: GraphQLRequest| {
            let schema = schema.clone();
            let filter = filter.clone();
            async move {
                let request = request.into_inner();
                let mut result = match filter(&request) {
                    Ok(()) => schema.execute(request).await,
                    Err(error) => async_graphql::Response::from_errors(vec![error]),
                };
                result.extensions.insert(
                    "originProof".into(),
                    async_graphql::value!({"generation": "g7", "opaque": "unchanged"}),
                );
                GraphQLResponse::from(result)
            }
        }),
    )
}

#[tokio::test]
async fn embedded_remote_parity() {
    let caps = GraphqlCapabilities {
        queries: true,
        ..Default::default()
    };
    let embedded = EmbeddedGraphql::custom(custom_router, caps, ["extra-field".into()]).unwrap();
    let direct = serve(mounted(
        GraphqlBinding::Embedded(embedded.clone()),
        GraphqlExecutor::Embedded,
        caps,
        vec!["extra-field".into()],
    ))
    .await;
    // Custom fields are registered at the complete origin executor. The remote
    // binding has no local schema extensions or field federation.
    let origin = serve(custom_router(Arc::new(|_| Ok(())))).await;
    let remote = serve(mounted(
        GraphqlBinding::Remote(RemoteGraphql {
            live_path: None,
            ..Default::default()
        }),
        GraphqlExecutor::Remote {
            origin: origin.origin.clone(),
        },
        caps,
        vec![],
    ))
    .await;
    let client = reqwest::Client::new();
    for request in [
        json!({ "query": "query First { extensionValue(input: \"wrong\") } query Selected($input: String!) { chosen: extensionValue(input: $input) }", "operationName": "Selected", "variables": { "input": "selected" }, "extensions": { "requestTag": "opaque" } }),
        json!({ "query": "{ unknownField }" }),
        json!({ "query": "query A { extensionValue(input: \"a\") } query B { extensionValue(input: \"b\") }" }),
        json!({ "query": "mutation Forbidden { write }" }),
    ] {
        let left = client
            .post(format!("{}/graphql", direct.origin))
            .json(&request)
            .send()
            .await
            .unwrap();
        let right = client
            .post(format!("{}/graphql", remote.origin))
            .json(&request)
            .send()
            .await
            .unwrap();
        assert_eq!(left.status(), right.status());
        let left: Value = left.json().await.unwrap();
        let right: Value = right.json().await.unwrap();
        assert_eq!(left, right, "{request}");
        if request["operationName"] == "Selected" {
            assert_eq!(left["data"], json!({"chosen": "selected"}));
            assert_eq!(left["extensions"]["originProof"]["generation"], "g7");
        }
    }
    let mut ws_request = format!("{}/graphql/ws", remote.origin.replace("http:", "ws:"))
        .into_client_request()
        .unwrap();
    ws_request.headers_mut().insert(
        "sec-websocket-protocol",
        "graphql-transport-ws".parse().unwrap(),
    );
    assert!(tokio_tungstenite::connect_async(ws_request).await.is_err());
}

#[derive(
    Clone, Debug, serde::Serialize, serde::Deserialize, distributed::read_model::ReadModel,
)]
#[readmodel(table = "gateway_items", primary_key = ["id"])]
struct Item {
    id: String,
}
#[tokio::test]
async fn query_only_schema_is_built_without_command_or_subscription_roots() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    let engine = Arc::new(
        GraphqlEngine::builder(pool)
            .identity(IdentityConfig::dev_headers())
            .roles(&["user"])
            .anonymous_role("user")
            .subscriptions(false)
            .model::<Item>(ModelPermissions::new().grant("user", read().all_columns()))
            .build()
            .unwrap(),
    );
    let caps = GraphqlCapabilities {
        queries: true,
        ..Default::default()
    };
    let schema = engine.sdl_for_role("user").unwrap();
    assert!(!schema.contains("type Mutation"));
    assert!(!schema.contains("type Subscription"));
    let embedded = EmbeddedGraphql::new(engine, None, caps).unwrap();
    let server = serve(mounted(
        GraphqlBinding::Embedded(embedded),
        GraphqlExecutor::Embedded,
        caps,
        vec![],
    ))
    .await;
    let value: Value = reqwest::Client::new()
        .post(format!("{}/graphql", server.origin))
        .json(&json!({"query":"{ __schema { mutationType { name } subscriptionType { name } } }"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        value["data"]["__schema"],
        json!({"mutationType": null, "subscriptionType": null})
    );
}

struct Writes(Arc<AtomicUsize>);
#[Object]
impl Writes {
    async fn write(&self) -> i32 {
        self.0.fetch_add(1, Ordering::SeqCst);
        1
    }
}
struct Ticks;
#[Subscription]
impl Ticks {
    async fn ticks(&self) -> impl futures_util::Stream<Item = i32> {
        futures_util::stream::iter([1])
    }
}
type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
async fn ws_json(socket: &mut Socket) -> Value {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(&text).unwrap(),
                Message::Ping(bytes) => {
                    socket.send(Message::Pong(bytes)).await.unwrap();
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await
    .unwrap()
}
async fn connect(origin: &str) -> Socket {
    let mut request = format!("{}/graphql/ws", origin.replace("http:", "ws:"))
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        "graphql-transport-ws".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            json!({"type":"connection_init","payload":{}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(ws_json(&mut socket).await, json!({"type":"connection_ack"}));
    socket
}

#[tokio::test]
async fn remote_ws_checks_selected_operations_and_preserves_ids() {
    let writes = Arc::new(AtomicUsize::new(0));
    let schema = Schema::build(CustomQuery, Writes(writes.clone()), Ticks).finish();
    let origin = serve(Router::new().route(
        "/graphql/ws",
        get(
            move |protocol: GraphQLProtocol, upgrade: WebSocketUpgrade| {
                let schema = schema.clone();
                async move {
                    upgrade
                        .protocols(async_graphql::http::ALL_WEBSOCKET_PROTOCOLS)
                        .on_upgrade(move |socket| {
                            GraphQLWebSocket::new(socket, schema, protocol).serve()
                        })
                        .into_response()
                }
            },
        ),
    ))
    .await;
    let caps = GraphqlCapabilities {
        queries: true,
        live: true,
        commands: false,
    };
    let remote = serve(mounted(
        GraphqlBinding::Remote(RemoteGraphql::default()),
        GraphqlExecutor::Remote {
            origin: origin.origin.clone(),
        },
        caps,
        vec![],
    ))
    .await;
    let mut socket = connect(&remote.origin).await;
    socket.send(Message::Text(json!({"id":"query-id","type":"subscribe","payload":{"query":"query Wrong { extensionValue(input: \"wrong\") } query Right($value:String!) { extensionValue(input:$value) }", "operationName":"Right", "variables":{"value":"right"}}}).to_string().into())).await.unwrap();
    let next = ws_json(&mut socket).await;
    assert_eq!(next["id"], "query-id");
    assert_eq!(next["payload"]["data"], json!({"extensionValue":"right"}));
    assert_eq!(ws_json(&mut socket).await["type"], "complete");
    socket
        .send(Message::Text(
            json!({"id":"mutation-id","type":"subscribe","payload":{"query":"mutation { write }"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let denied = ws_json(&mut socket).await;
    assert_eq!(denied["id"], "mutation-id");
    assert_eq!(
        denied["payload"]["errors"][0]["extensions"]["code"],
        "OPERATION_NOT_MOUNTED"
    );
    assert_eq!(ws_json(&mut socket).await["type"], "complete");
    socket
        .send(Message::Text(
            json!({"id":"live-id","type":"subscribe","payload":{"query":"subscription { ticks }"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        ws_json(&mut socket).await["payload"]["data"],
        json!({"ticks":1})
    );
    assert_eq!(ws_json(&mut socket).await["type"], "complete");
    socket.close(None).await.unwrap();
    let mut binary = connect(&remote.origin).await;
    binary
        .send(Message::Binary(
            json!({"id":"binary","type":"subscribe","payload":{"query":"mutation { write }"}})
                .to_string()
                .into_bytes()
                .into(),
        ))
        .await
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(2), binary.next())
        .await
        .unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reloading_response_is_preserved_without_retry_or_receipt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let origin = serve(Router::new().fallback(move || { let calls = counted.clone(); async move {
        calls.fetch_add(1, Ordering::SeqCst);
        (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({"errors":[{"message":"application generation is reloading", "extensions":{"code":"APPLICATION_RELOADING"}}]})))
    }})).await;
    let caps = GraphqlCapabilities {
        commands: true,
        queries: true,
        live: false,
    };
    let remote = serve(mounted(
        GraphqlBinding::Remote(RemoteGraphql::default()),
        GraphqlExecutor::Remote {
            origin: origin.origin.clone(),
        },
        caps,
        vec![],
    ))
    .await;
    let response = reqwest::Client::new().post(format!("{}/graphql", remote.origin)).json(&json!({"query":"mutation Write($commandId: ID!) { write(commandId:$commandId) }", "variables":{"commandId":"exact-id"}})).send().await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body: Value = response.json().await.unwrap();
    assert_eq!(
        body,
        json!({"errors":[{"message":"application generation is reloading", "extensions":{"code":"APPLICATION_RELOADING"}}]})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn timed_out_mutation_executes_once_and_returns_no_receipt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let origin = serve(Router::new().fallback(move || {
        let calls = counted.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
            StatusCode::OK
        }
    }))
    .await;
    let caps = GraphqlCapabilities {
        commands: true,
        queries: true,
        live: false,
    };
    let mut options = NativeOptions::new("https://public.example.invalid");
    options.limits.response_header_timeout = Duration::from_millis(100);
    let remote = serve(mounted_with_options(
        GraphqlBinding::Remote(RemoteGraphql::default()),
        GraphqlExecutor::Remote {
            origin: origin.origin.clone(),
        },
        caps,
        vec![],
        options,
    ))
    .await;
    let response = reqwest::Client::new()
        .post(format!("{}/graphql", remote.origin))
        .json(&json!({"query":"mutation { write }"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(response.bytes().await.unwrap().is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// What a GraphQL origin sees of a client request: its own path, the headers
/// the gateway allows, the provider's credential, and no gateway control.
#[tokio::test]
async fn graphql_origin_receives_no_client_identity_channel() {
    type Seen = Arc<std::sync::Mutex<Vec<Value>>>;
    let seen: Seen = Arc::default();
    async fn record(
        axum::extract::State(seen): axum::extract::State<Seen>,
        request: axum::http::Request<axum::body::Body>,
    ) -> axum::Json<Value> {
        let (parts, body) = request.into_parts();
        let body = axum::body::to_bytes(body, 65536).await.unwrap();
        let headers: std::collections::BTreeMap<_, _> = parts
            .headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect();
        seen.lock().unwrap().push(json!({
            "target": parts.uri.to_string(),
            "headers": headers,
            "body": serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null),
        }));
        axum::Json(json!({"data":{"__typename":"Query"}}))
    }
    let origin = serve(
        Router::new()
            .route("/graphql", post(record))
            .route("/graphql/ws", get(record))
            .with_state(seen.clone()),
    )
    .await;
    let caps = GraphqlCapabilities {
        queries: true,
        commands: true,
        live: true,
    };
    let config = GatewayConfig {
        bindings: vec![Binding::new(
            "graphql",
            BindingKind::Graphql {
                executor: GraphqlExecutor::Remote {
                    origin: origin.origin.clone(),
                },
                capabilities: caps,
                delivery: DeliveryCapabilities::default(),
                schema_extensions: vec![],
            },
        )],
        routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "graphql")],
    }
    .build()
    .unwrap();
    let auth = NativeAuth::new(|credentials| async move {
        assert_eq!(credentials.authorization.as_deref(), Some("Bearer client"));
        RequestContext::from_provider(
            Some(Identity::verified("local", "alice", vec!["user".into()], u64::MAX).unwrap()),
            "test-v1",
            BackendCredential::Bearer("backend".into()),
        )
        .map_err(|_| AuthError::Unavailable)
    });
    let gateway = serve(
        NativeGateway::new(
            config,
            NativeOptions::new("https://public.example.invalid"),
            [(
                "graphql".into(),
                NativeBinding::Graphql(GraphqlBinding::Remote(RemoteGraphql::default())),
            )],
            auth,
        )
        .unwrap()
        .router(),
    )
    .await;

    let forged = [
        ("authorization", "Bearer client"),
        ("cookie", "session=1"),
        ("x-user-id", "mallory"),
        ("x-roles", "admin"),
        ("x-application-secret", "guess"),
    ];
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!(
            "{}/graphql?x-user-id=mallory&x-roles=admin",
            gateway.origin
        ))
        .json(&json!({
            "query": "{ __typename }",
            "extensions": {
                "kept": true,
                "gatewayDelivery": {
                    "action": "validate",
                    "connectionInit": {"x-user-id": "mallory", "x-roles": "admin"},
                },
            },
        }));
    for (name, value) in forged {
        request = request.header(name, value);
    }
    assert_eq!(request.send().await.unwrap().status(), StatusCode::OK);

    let mut upgrade = client
        .get(format!(
            "{}/graphql?x-user-id=mallory&x-roles=admin",
            gateway.origin
        ))
        .header("connection", "upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-protocol", "graphql-transport-ws");
    for (name, value) in forged {
        upgrade = upgrade.header(name, value);
    }
    // The fixture origin answers the handshake with plain JSON; the gateway
    // relays that refusal.
    assert_eq!(upgrade.send().await.unwrap().status(), StatusCode::OK);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0]["target"], "/graphql");
    assert_eq!(seen[1]["target"], "/graphql/ws");
    assert_eq!(
        seen[0]["body"]["extensions"],
        json!({"kept": true}),
        "a client cannot write the gateway's control extension"
    );
    for request in &seen {
        let headers = request["headers"].as_object().unwrap();
        assert_eq!(headers["authorization"], "Bearer backend");
        for name in headers.keys() {
            assert!(
                GRAPHQL_REQUEST_HEADERS.contains(&name.as_str())
                    || [
                        "authorization",
                        "host",
                        "x-forwarded-host",
                        "x-forwarded-proto",
                        "connection",
                        "upgrade",
                        // Framing of the gateway's own request to the origin.
                        "transfer-encoding",
                    ]
                    .contains(&name.as_str()),
                "origin received `{name}`"
            );
        }
    }
}

/// A remote GraphQL origin serving live operations over
/// `graphql-transport-ws`; `admit` decides its `connection_init`.
async fn live_origin(admit: bool) -> Server {
    let schema = Schema::build(CustomQuery, EmptyMutation, EmptySubscription).finish();
    serve(Router::new().route(
        "/graphql/ws",
        get(
            move |protocol: GraphQLProtocol, upgrade: WebSocketUpgrade| {
                let schema = schema.clone();
                async move {
                    upgrade
                        .protocols(async_graphql::http::ALL_WEBSOCKET_PROTOCOLS)
                        .on_upgrade(move |socket| {
                            GraphQLWebSocket::new(socket, schema, protocol)
                                .on_connection_init(move |_| async move {
                                    if admit {
                                        Ok(async_graphql::Data::default())
                                    } else {
                                        Err(async_graphql::Error::new("not admitted"))
                                    }
                                })
                                .serve()
                        })
                        .into_response()
                }
            },
        ),
    ))
    .await
}

/// A gateway in front of `origin` that shares live operations, so the
/// gateway itself terminates the client's WebSocket.
async fn live_gateway(origin: &Server) -> Server {
    let caps = GraphqlCapabilities {
        queries: true,
        live: true,
        commands: false,
    };
    let gateway = GatewayConfig {
        bindings: vec![Binding::new(
            "graphql",
            BindingKind::Graphql {
                executor: GraphqlExecutor::Remote {
                    origin: origin.origin.clone(),
                },
                capabilities: caps,
                delivery: DeliveryCapabilities {
                    live_sharing: true,
                    ..Default::default()
                },
                schema_extensions: vec![],
            },
        )],
        routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "graphql")],
    }
    .build()
    .unwrap();
    let delivery = Arc::new(
        NativeDelivery::live(distributed::gateway::delivery::LiveLimits::default()).unwrap(),
    );
    serve(
        NativeGateway::new(
            gateway,
            NativeOptions::new("https://public.example.invalid"),
            [(
                "graphql".into(),
                NativeBinding::GraphqlWithDelivery(
                    GraphqlBinding::Remote(RemoteGraphql::default()),
                    delivery,
                ),
            )],
            NativeAuth::anonymous(),
        )
        .unwrap()
        .router(),
    )
    .await
}

/// The next frame that is not a text frame, or the close frame's code and
/// reason; `None` when the stream ends without one.
async fn next_control(socket: &mut Socket) -> Option<Message> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match socket.next().await? {
                Ok(Message::Text(_)) => continue,
                Ok(other) => return Some(other),
                Err(_) => return None,
            }
        }
    })
    .await
    .unwrap()
}

fn close_code(message: Option<Message>) -> Option<(u16, String)> {
    match message {
        Some(Message::Close(Some(frame))) => Some((frame.code.into(), frame.reason.to_string())),
        _ => None,
    }
}

/// A gateway that shares live operations terminates the client's
/// `graphql-transport-ws` connection itself: it answers keep-alives, ignores
/// frames that change nothing, and closes the connection on a protocol
/// violation with a code telling the client to reset.
#[tokio::test]
async fn the_gateway_answers_keepalives_and_closes_on_a_protocol_violation() {
    let origin = live_origin(true).await;
    let gateway = live_gateway(&origin).await;

    let mut socket = connect(&gateway.origin).await;
    socket
        .send(Message::Text(
            json!({"type":"ping","payload":{"n":1}}).to_string().into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        ws_json(&mut socket).await,
        json!({"type":"pong","payload":{"n":1}})
    );
    // A pong and a complete for an operation never opened are no-ops: the
    // next frame the client receives answers its next ping.
    for frame in [
        json!({"type":"pong"}),
        json!({"type":"complete","id":"never-opened"}),
    ] {
        socket
            .send(Message::Text(frame.to_string().into()))
            .await
            .unwrap();
    }
    socket
        .send(Message::Text(
            json!({"type":"ping","payload":{"n":2}}).to_string().into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        ws_json(&mut socket).await,
        json!({"type":"pong","payload":{"n":2}})
    );
    socket
        .send(Message::Ping(b"alive".to_vec().into()))
        .await
        .unwrap();
    assert!(matches!(
        next_control(&mut socket).await,
        Some(Message::Pong(bytes)) if &bytes[..] == b"alive"
    ));

    let violations: [(&str, Message); 4] = [
        (
            "a second connection_init",
            Message::Text(
                json!({"type":"connection_init","payload":{}})
                    .to_string()
                    .into(),
            ),
        ),
        ("text that is not JSON", Message::Text("{".into())),
        (
            "a frame type the protocol does not have",
            Message::Text(json!({"type":"start","id":"1"}).to_string().into()),
        ),
        ("a binary frame", Message::Binary(b"{}".to_vec().into())),
    ];
    for (name, frame) in violations {
        let mut socket = connect(&gateway.origin).await;
        socket.send(frame).await.unwrap();
        assert_eq!(
            close_code(next_control(&mut socket).await),
            Some((1013, "LIVE_RESET_REQUIRED".into())),
            "{name}"
        );
    }
}

/// An origin that refuses the client's `connection_init` decides the
/// connection: the sharing gateway passes the origin's close frame on to
/// the client.
#[tokio::test]
async fn an_origin_that_refuses_connection_init_closes_the_client() {
    let origin = live_origin(false).await;
    let gateway = live_gateway(&origin).await;
    let mut request = format!("{}/graphql/ws", gateway.origin.replace("http:", "ws:"))
        .into_client_request()
        .unwrap();
    request.headers_mut().insert(
        "sec-websocket-protocol",
        "graphql-transport-ws".parse().unwrap(),
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    socket
        .send(Message::Text(
            json!({"type":"connection_init","payload":{}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let close = close_code(next_control(&mut socket).await);
    // The origin's own close frame, as async-graphql sends it.
    assert_eq!(close, Some((1002, "not admitted".into())));
}

/// One GraphQL binding declared with `declared` and bound at runtime to
/// `bound`, served on `https://public.example.invalid` with `options`.
fn native_graphql(
    declared: BindingKind,
    bound: Vec<NativeBinding>,
    options: NativeOptions,
) -> Result<NativeGateway, GatewayError> {
    let gateway = GatewayConfig {
        bindings: vec![Binding::new("graphql", declared)],
        routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "graphql")],
    }
    .build()
    .unwrap();
    NativeGateway::new(
        gateway,
        options,
        bound
            .into_iter()
            .map(|binding| ("graphql".to_string(), binding)),
        NativeAuth::anonymous(),
    )
}

/// Every runtime GraphQL binding is checked against its declaration before
/// the gateway serves: a contradiction is a configuration error, never a
/// request-time surprise.
#[tokio::test]
async fn a_native_gateway_refuses_a_graphql_binding_that_contradicts_its_declaration() {
    let caps = GraphqlCapabilities {
        queries: true,
        live: true,
        commands: false,
    };
    let declared =
        |executor: GraphqlExecutor, delivery: DeliveryCapabilities| BindingKind::Graphql {
            executor,
            capabilities: caps,
            delivery,
            schema_extensions: vec![],
        };
    let remote_at = |origin: &str| GraphqlExecutor::Remote {
        origin: origin.into(),
    };
    let elsewhere = || remote_at("https://origin.example.invalid");
    let remote = |binding: RemoteGraphql| NativeBinding::Graphql(GraphqlBinding::Remote(binding));
    let options = || NativeOptions::new("https://public.example.invalid");
    let live = || {
        Arc::new(
            NativeDelivery::live(distributed::gateway::delivery::LiveLimits::default()).unwrap(),
        )
    };
    let sharing = DeliveryCapabilities {
        live_sharing: true,
        ..Default::default()
    };
    let embedded = EmbeddedGraphql::custom(
        custom_router,
        GraphqlCapabilities {
            queries: true,
            ..Default::default()
        },
        [],
    )
    .unwrap();

    let cases: Vec<(&str, BindingKind, Vec<NativeBinding>, NativeOptions, &str)> = vec![
        (
            "a zero request body limit",
            declared(elsewhere(), DeliveryCapabilities::default()),
            vec![remote(RemoteGraphql::default())],
            {
                let mut options = options();
                options.limits.request_body_bytes = 0;
                options
            },
            "invalid native proxy limits",
        ),
        (
            "a header to strip that is not a header name",
            declared(elsewhere(), DeliveryCapabilities::default()),
            vec![remote(RemoteGraphql::default())],
            {
                let mut options = options();
                options.strip_headers = vec!["not a header".into()];
                options
            },
            "invalid identity header strip list",
        ),
        (
            "a remote binding for an embedded declaration",
            declared(GraphqlExecutor::Embedded, DeliveryCapabilities::default()),
            vec![remote(RemoteGraphql::default())],
            options(),
            "GraphQL executor location mismatch",
        ),
        (
            "an embedded schema registered with other capabilities",
            declared(GraphqlExecutor::Embedded, DeliveryCapabilities::default()),
            vec![NativeBinding::Graphql(GraphqlBinding::Embedded(embedded))],
            options(),
            "embedded schema registration does not match declaration",
        ),
        (
            "a remote path with a parent segment",
            declared(elsewhere(), DeliveryCapabilities::default()),
            vec![remote(RemoteGraphql {
                http_path: "/graphql/../admin".into(),
                ..Default::default()
            })],
            options(),
            "ambiguous gateway path segments",
        ),
        (
            "an origin that is the gateway itself",
            declared(
                remote_at("https://public.example.invalid"),
                DeliveryCapabilities::default(),
            ),
            vec![remote(RemoteGraphql::default())],
            options(),
            "GraphQL upstream points to gateway",
        ),
        (
            "a remote path with a needless percent-encoding",
            declared(elsewhere(), DeliveryCapabilities::default()),
            vec![remote(RemoteGraphql {
                http_path: "/graph%71l".into(),
                ..Default::default()
            })],
            options(),
            "invalid remote GraphQL path",
        ),
        (
            "live capability without a remote live endpoint",
            declared(elsewhere(), DeliveryCapabilities::default()),
            vec![remote(RemoteGraphql {
                live_path: None,
                ..Default::default()
            })],
            options(),
            "live gateway needs a remote live endpoint",
        ),
        (
            "live sharing declared without a delivery resource",
            declared(elsewhere(), sharing),
            vec![remote(RemoteGraphql::default())],
            options(),
            "delivery adapter is not bound",
        ),
        (
            "a delivery resource the declaration does not ask for",
            declared(elsewhere(), DeliveryCapabilities::default()),
            vec![NativeBinding::GraphqlWithDelivery(
                GraphqlBinding::Remote(RemoteGraphql::default()),
                live(),
            )],
            options(),
            "delivery resource does not match declaration",
        ),
        (
            "the same binding bound twice",
            declared(elsewhere(), DeliveryCapabilities::default()),
            vec![
                remote(RemoteGraphql::default()),
                remote(RemoteGraphql::default()),
            ],
            options(),
            "duplicate native binding",
        ),
    ];
    for (name, declaration, bound, options, expected) in cases {
        match native_graphql(declaration, bound, options) {
            Err(error) => assert_eq!(error.to_string(), expected, "{name}"),
            Ok(_) => panic!("{name}: expected `{expected}`"),
        }
    }
}

/// The gateway reads a GraphQL HTTP request itself before any executor
/// sees it: a request it cannot read is answered at the edge.
#[tokio::test]
async fn a_graphql_request_the_gateway_cannot_read_is_answered_at_the_edge() {
    let caps = GraphqlCapabilities {
        queries: true,
        ..Default::default()
    };
    let embedded = EmbeddedGraphql::custom(custom_router, caps, []).unwrap();
    let mut options = NativeOptions::new("https://public.example.invalid");
    options.limits.request_body_bytes = 64;
    let server = serve(mounted_with_options(
        GraphqlBinding::Embedded(embedded),
        GraphqlExecutor::Embedded,
        caps,
        vec![],
        options,
    ))
    .await;
    let client = reqwest::Client::new();
    let url = format!("{}/graphql", server.origin);

    let get = client.get(&url).send().await.unwrap();
    assert_eq!(get.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(get.headers()["allow"], "POST");

    let oversized = client
        .post(&url)
        .body(format!(r#"{{"query":"{{ {} }}"}}"#, "x".repeat(128)))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let unreadable = client.post(&url).body("{not json").send().await.unwrap();
    let body: Value = unreadable.json().await.unwrap();
    assert_eq!(body["errors"][0]["extensions"]["code"], "BAD_REQUEST");
}
