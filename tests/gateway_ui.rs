#![cfg(feature = "gateway-native")]
use axum::{
    body::{Body, Bytes},
    extract::{
        ws::{Message, WebSocketUpgrade},
        State,
    },
    http::{header, HeaderMap, HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
    Router,
};
use distributed::gateway::{native::*, *};
use futures_util::{SinkExt, Stream, StreamExt};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, Notify},
    task::JoinHandle,
};
use tower::ServiceExt;

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
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Server { origin, task }
}
fn auth() -> NativeAuth {
    NativeAuth::new(|credentials| async move {
        let identity = match credentials.authorization.as_deref() {
            None => None,
            Some("Bearer valid") => {
                Some(Identity::verified("local", "alice", vec!["user".into()], u64::MAX).unwrap())
            }
            _ => return Err(AuthError::Unauthorized),
        };
        RequestContext::from_provider(identity, "test-v1", BackendCredential::None)
            .map_err(|_| AuthError::Unavailable)
    })
}
fn gateway(origin: &str, upstream: &str, limits: ProxyLimits) -> NativeGateway {
    let mut private = Route::new("private", RoutePath::prefix("/private"), "ui");
    private.admission = vec!["auth".into()];
    let config = GatewayConfig {
        bindings: vec![
            Binding::new(
                "ui",
                BindingKind::UiProxy {
                    origin: upstream.into(),
                },
            ),
            Binding::new("auth", BindingKind::Admission),
        ],
        routes: vec![private, Route::new("ui", RoutePath::prefix("/"), "ui")],
    }
    .build()
    .unwrap();
    let mut options = NativeOptions::new(origin);
    options.limits = limits;
    options.strip_headers = vec!["x-company-user".into(), "x-gateway-secret".into()];
    NativeGateway::new(
        config,
        options,
        vec![
            ("ui".into(), NativeBinding::UiProxy { websocket: true }),
            (
                "auth".into(),
                NativeBinding::Admission(Admission::Authenticated),
            ),
        ],
        auth(),
    )
    .unwrap()
}

struct TrackedStream {
    receiver: mpsc::Receiver<Result<Bytes, Infallible>>,
    dropped: Arc<AtomicUsize>,
}
impl Stream for TrackedStream {
    type Item = Result<Bytes, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}
impl Drop for TrackedStream {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
struct Upstream {
    finish: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
    ws_closed: Arc<Notify>,
}
async fn stream(State(state): State<Upstream>) -> Response {
    let (send, receiver) = mpsc::channel(1);
    let dropped = state.dropped.clone();
    tokio::spawn(async move {
        if send.send(Ok(Bytes::from_static(b"first"))).await.is_err() {
            return;
        }
        tokio::select! { _ = state.finish.notified() => { let _ = send.send(Ok(Bytes::from_static(b"last"))).await; }, _ = send.closed() => {} }
    });
    Response::new(Body::from_stream(TrackedStream { receiver, dropped }))
}
async fn echo(request: Request<Body>) -> Response {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 1024).await.unwrap();
    let headers: BTreeMap<_, _> = parts
        .headers
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
        .collect();
    let mut result = axum::Json(serde_json::json!({ "method": parts.method.to_string(), "target": parts.uri.to_string(), "body": String::from_utf8(body.to_vec()).unwrap(), "headers": headers })).into_response();
    result.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_static("one=1; Path=/; HttpOnly; SameSite=Lax"),
    );
    result.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_static("two=2; Path=/; HttpOnly; SameSite=Lax"),
    );
    result.headers_mut().insert(
        header::CONNECTION,
        HeaderValue::from_static("x-hop-response"),
    );
    result
        .headers_mut()
        .insert("x-hop-response", HeaderValue::from_static("remove"));
    result
}
async fn websocket(State(state): State<Upstream>, ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(move |mut ws| async move {
        while let Some(Ok(message)) = ws.recv().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
            if ws.send(message).await.is_err() {
                break;
            }
        }
        state.ws_closed.notify_one();
    })
}

#[tokio::test]
async fn stream_cookies_redirects_and_upgrades() {
    let state = Upstream {
        finish: Arc::new(Notify::new()),
        dropped: Arc::new(AtomicUsize::new(0)),
        ws_closed: Arc::new(Notify::new()),
    };
    let upstream = serve(
        Router::new()
            .route("/stream", get(stream))
            .route("/ws", get(websocket))
            .route("/echo", any(echo))
            .route("/head", get(|| async { "head-body" }))
            .with_state(state.clone()),
    )
    .await;
    let ingress_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public = format!("http://{}", ingress_listener.local_addr().unwrap());
    let router = gateway(&public, &upstream.origin, ProxyLimits::default()).router();
    let ingress = Server {
        origin: public.clone(),
        task: tokio::spawn(async move { axum::serve(ingress_listener, router).await.unwrap() }),
    };
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let response = client
        .post(format!("{public}/echo?x=a%2Fb"))
        .header("x-user-id", "admin")
        .header("x-hasura-role", "admin")
        .header("x-forwarded-host", "attacker.invalid")
        .header("forwarded", "host=attacker.invalid")
        .header("x-company-user", "admin")
        .header("x-gateway-secret", "spoofed")
        .header("connection", "x-hop-request")
        .header("x-hop-request", "remove")
        .header("origin", &public)
        .body("input")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response
            .headers()
            .get_all(header::SET_COOKIE)
            .iter()
            .count(),
        2
    );
    assert!(!response.headers().contains_key("x-hop-response"));
    let value: serde_json::Value = response.json().await.unwrap();
    assert_eq!(value["method"], "POST");
    assert_eq!(value["target"], "/echo?x=a%2Fb");
    assert_eq!(value["body"], "input");
    assert_eq!(
        value["headers"]["host"],
        public.trim_start_matches("http://")
    );
    assert_eq!(
        value["headers"]["x-forwarded-host"],
        public.trim_start_matches("http://")
    );
    assert_eq!(value["headers"]["origin"], public);
    for name in [
        "x-user-id",
        "x-hasura-role",
        "forwarded",
        "x-company-user",
        "x-gateway-secret",
        "x-hop-request",
    ] {
        assert!(value["headers"].get(name).is_none(), "{name}");
    }
    let head = client.head(format!("{public}/head")).send().await.unwrap();
    assert_eq!(head.headers()[header::CONTENT_LENGTH], "9");
    assert!(head.bytes().await.unwrap().is_empty());
    let mut response = client.get(format!("{public}/stream")).send().await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), response.chunk())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first, "first"); // Completion is impossible until this signal.
    state.finish.notify_one();
    assert_eq!(response.bytes().await.unwrap(), "last");
    let mut response = client.get(format!("{public}/stream")).send().await.unwrap();
    assert_eq!(response.chunk().await.unwrap().unwrap(), "first");
    drop(response);
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.dropped.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("{}/ws", ingress.origin.replace("http:", "ws:")))
            .await
            .unwrap();
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        "hello".into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap().into_text().unwrap(),
        "hello"
    );
    ws.close(None).await.unwrap();
    drop(ws);
    tokio::time::timeout(Duration::from_secs(2), state.ws_closed.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn redirect_ownership_assets_and_limits() {
    let upstream = serve(Router::new().fallback(|headers: HeaderMap| async move {
        let mut response = StatusCode::FOUND.into_response();
        // App emits a private absolute URL; it must be rewritten without following.
        response
            .headers_mut()
            .insert(header::LOCATION, headers["x-redirect-to"].clone());
        response
    }))
    .await;
    let ingress = serve(
        gateway(
            "https://site.test",
            &upstream.origin,
            ProxyLimits {
                request_body_bytes: 4,
                ..Default::default()
            },
        )
        .router(),
    )
    .await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let response = client
        .get(&ingress.origin)
        .header(
            "x-redirect-to",
            format!("{}/next?q=1#anchor", upstream.origin),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        response.headers()[header::LOCATION],
        "https://site.test/next?q=1#anchor"
    );
    let response = client
        .get(&ingress.origin)
        .header("x-redirect-to", "https://idp.example.invalid/authorize")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::LOCATION],
        "https://idp.example.invalid/authorize"
    );
    let response = client
        .get(&ingress.origin)
        .header(
            "x-redirect-to",
            format!("//{}/next", upstream.origin.trim_start_matches("http://")),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.headers()[header::LOCATION],
        "https://site.test/next"
    );
    assert_eq!(
        client
            .post(&ingress.origin)
            .body("large")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        client
            .get(format!("{}/private/page", ingress.origin))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(&ingress.origin)
            .header("authorization", "Bearer forged")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );

    let assets = StaticAssets::new(
        [(
            "/private/file".into(),
            Asset {
                bytes: Bytes::from_static(b"secret"),
                content_type: HeaderValue::from_static("text/plain"),
            },
        )],
        6,
    )
    .unwrap();
    let mut route = Route::new("asset", RoutePath::prefix("/private"), "asset");
    route.admission.push("auth".into());
    let mut api = Route::new("api", RoutePath::prefix("/api"), "api");
    api.methods = Methods::Only(vec!["POST".into()]);
    let config = GatewayConfig {
        bindings: vec![
            Binding::new("asset", BindingKind::Assets),
            Binding::new("auth", BindingKind::Admission),
            Binding::new("api", BindingKind::Handler),
        ],
        routes: vec![route, api, {
            let mut custom = Route::new("custom", RoutePath::exact("/custom"), "api");
            custom.admission.push("auth".into());
            custom
        }],
    }
    .build()
    .unwrap();
    let router = NativeGateway::new(
        config,
        NativeOptions::new("https://site.test"),
        vec![
            ("asset".into(), NativeBinding::Assets(assets)),
            (
                "auth".into(),
                NativeBinding::Admission(Admission::Authenticated),
            ),
            (
                "api".into(),
                NativeBinding::Handler(
                    Router::new().route("/custom", get(|axum::Extension(context): axum::Extension<RequestContext>| async move { context.identity().unwrap().subject().to_string() })).fallback(|| async { StatusCode::SERVICE_UNAVAILABLE }),
                ),
            ),
        ],
        auth(),
    )
    .unwrap()
    .router();
    for (method, path, token, expected) in [
        ("GET", "/private/file", None, 401),
        ("GET", "/custom", None, 401),
        ("GET", "/custom", Some("Bearer valid"), 200),
        ("GET", "/private/file", Some("Bearer valid"), 200),
        ("HEAD", "/private/file", Some("Bearer valid"), 200),
        ("GET", "/private/%2e%2e/file", Some("Bearer valid"), 400),
        ("GET", "/api/missing", None, 405),
        ("POST", "/api/missing", None, 503),
    ] {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            request = request.header("authorization", token);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), expected, "{method} {path}");
        if path == "/custom" && expected == 200 {
            assert_eq!(
                axum::body::to_bytes(response.into_body(), 100)
                    .await
                    .unwrap(),
                "alice"
            );
            continue;
        }
        if method == "HEAD" {
            assert_eq!(response.headers()[header::CONTENT_LENGTH], "6");
            assert!(axum::body::to_bytes(response.into_body(), 100)
                .await
                .unwrap()
                .is_empty());
        }
    }
}

#[test]
fn native_bindings_reject_loops_and_duplicate_resources() {
    let config = || {
        GatewayConfig {
            bindings: vec![Binding::new(
                "ui",
                BindingKind::UiProxy {
                    origin: "https://site.test".into(),
                },
            )],
            routes: vec![Route::new("ui", RoutePath::prefix("/"), "ui")],
        }
        .build()
        .unwrap()
    };
    assert!(NativeGateway::new(
        config(),
        NativeOptions::new("https://site.test"),
        [("ui".into(), NativeBinding::UiProxy { websocket: false })],
        NativeAuth::anonymous()
    )
    .is_err());
    assert!(NativeGateway::new(
        config(),
        NativeOptions::new("https://other.test"),
        [
            ("ui".into(), NativeBinding::UiProxy { websocket: false }),
            ("ui".into(), NativeBinding::UiProxy { websocket: false })
        ],
        NativeAuth::anonymous()
    )
    .is_err());
    assert!(StaticAssets::new(
        [(
            "/../secret".into(),
            Asset {
                bytes: Bytes::new(),
                content_type: HeaderValue::from_static("text/plain")
            }
        )],
        100
    )
    .is_err());
}

#[tokio::test]
async fn capacity_cancellation_timeout_and_alias_loop_are_bounded() {
    let state = Upstream {
        finish: Arc::new(Notify::new()),
        dropped: Arc::new(AtomicUsize::new(0)),
        ws_closed: Arc::new(Notify::new()),
    };
    let upstream = serve(
        Router::new()
            .route("/stream", get(stream))
            .route(
                "/slow",
                get(|| async {
                    std::future::pending::<()>().await;
                    "unreachable"
                }),
            )
            .with_state(state.clone()),
    )
    .await;
    let ingress = serve(
        gateway(
            "https://site.test",
            &upstream.origin,
            ProxyLimits {
                concurrent_requests: 1,
                response_header_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .router(),
    )
    .await;
    let client = reqwest::Client::new();
    let mut first = client
        .get(format!("{}/stream", ingress.origin))
        .send()
        .await
        .unwrap();
    assert_eq!(first.chunk().await.unwrap().unwrap(), "first");
    assert_eq!(
        client
            .get(format!("{}/stream", ingress.origin))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    drop(first);
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        client
            .get(format!("{}/slow", ingress.origin))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::GATEWAY_TIMEOUT
    );

    // Two names can resolve to one listener. A bounded hop chain catches this
    // even when origin string comparison at construction cannot detect it.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let direct = format!("http://{}", listener.local_addr().unwrap());
    let router = gateway("https://alias.test", &direct, ProxyLimits::default()).router();
    let looped = Server {
        origin: direct,
        task: tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    };
    let response = tokio::time::timeout(Duration::from_secs(2), client.get(&looped.origin).send())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::LOOP_DETECTED);
}
