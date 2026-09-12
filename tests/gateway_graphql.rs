#![cfg(all(feature = "gateway-graphql-native", feature = "sqlite"))]
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
fn custom_router(filter: distributed::graphql::GraphqlOperationFilter) -> Router {
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, distributed::ReadModel)]
#[readmodel(table = "gateway_items", primary_key = ["id"])]
struct Item {
    id: String,
}
#[tokio::test]
async fn query_only_schema_is_built_without_command_or_subscription_roots() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    let engine = Arc::new(
        GraphqlEngine::builder(pool)
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
