use super::*;

#[tokio::test]
#[ignore = "runs actual 100-query/100-live native and workerd load matrix; requires pinned Worker fixture"]
async fn separate_origin_and_client_savings() {
    use axum::Router;
    use distributed::graphql::{delivery::GatewayVersionStore, IdentityConfig, OidcConfig};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use rsa::{pkcs1::EncodeRsaPrivateKey, traits::PublicKeyParts, RsaPrivateKey, RsaPublicKey};
    let private = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let public = RsaPublicKey::from(&private);
    let encoding = EncodingKey::from_rsa_pem(
        private
            .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    let jwks=json!({"keys":[{"kty":"RSA","kid":"live-test","alg":"RS256","use":"sig",
        "n":base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),"e":base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(public.e().to_bytes_be())}]}).to_string();
    let token = |subject: &str| {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("live-test".into());
        encode(&header,&json!({"iss":"https://live-fixture.invalid","aud":"live-fixture","sub":subject,"iat":now-1,"nbf":now-1,"exp":now+3600,"roles":["user"]}),&encoding).unwrap()
    };

    let fixture = protocol_fixture_with_retention(5).await;
    let versions = GatewayVersionStore::install(
        &distributed::graphql::GraphqlPool::from(fixture.repository.pool().clone()),
        "load-fixture",
        ["causal_query_views".into()],
    )
    .await
    .unwrap();
    let oidc = OidcConfig::new("https://live-fixture.invalid", "live-fixture")
        .with_static_jwks(jwks)
        .engine_roles(&["user"]);
    let engine = Arc::new(
        GraphqlEngine::builder(&fixture.repository)
            .service_id(SERVICE_ID)
            .protocol_token_key(TEST_PROTOCOL_TOKEN_KEY)
            .roles(&["user"])
            .identity(IdentityConfig::oidc_bearer(oidc))
            .model::<CausalQueryView>(ModelPermissions::new().grant("user", read().all_columns()))
            .client_projectors([projector()])
            .change_stream(fixture.repository.read_model_changes())
            .gateway_versions(versions.clone())
            .build()
            .unwrap(),
    );

    use axum::{
        body::Body,
        extract::Request as HttpRequest,
        middleware::{self, Next},
        routing::{get, post},
    };
    use distributed::gateway::{delivery::*, native::*, *};
    use std::collections::BTreeMap;
    let gate = Arc::new(tokio::sync::Semaphore::new(10000));
    let gate_layer = gate.clone();
    let gql = distributed::graphql::graphql_router_composed(engine.clone(), None, None).layer(
        middleware::from_fn(move |request: HttpRequest, next: Next| {
            let gate = gate_layer.clone();
            async move {
                if request.method() != axum::http::Method::POST {
                    return next.run(request).await;
                }
                let (parts, body) = request.into_parts();
                let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
                let value: Value = serde_json::from_slice(&bytes).unwrap();
                if value["extensions"]["gatewayDelivery"]["action"] != "validate" {
                    gate.acquire().await.unwrap().forget();
                }
                next.run(HttpRequest::from_parts(parts, Body::from(bytes)))
                    .await
            }
        }),
    );
    let observed = engine.clone();
    let counters = versions.clone();
    let release = gate.clone();
    let repository = fixture.repository.clone();
    let bus = fixture.bus.clone();
    let control = Router::new()
        .route("/__metrics", get(move || {let engine=observed.clone();let versions=counters.clone();async move {let m=versions.metrics();axum::Json(json!({"producers":engine.live_subscriber_count(),"validations":m.validations,"resultExecutions":m.result_executions}))}}))
        .route("/__block", post(move || {let gate=gate.clone();async move {gate.forget_permits(gate.available_permits());"blocked"}}))
        .route("/__release", post(move || {let gate=release.clone();async move {gate.add_permits(10000);"released"}}))
        .route("/__next/{position}",post(move |axum::extract::Path(position):axum::extract::Path<u64>| {let repository=repository.clone();let bus=bus.clone();async move {project_item(&repository,&bus,position,&format!("load-{position}-{}","x".repeat(4096))).await;"committed"}}));
    struct Server {
        origin: String,
        task: tokio::task::JoinHandle<()>,
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
    let origin = serve(gql.merge(control)).await;
    // Reserve an ephemeral metering proxy port for the owned Node runner.
    let meter = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let meter_port = meter.local_addr().unwrap().port();
    drop(meter);
    let mut servers = Vec::new();
    let mut native = BTreeMap::new();
    for mode in ["none", "flights", "snapshots", "live"] {
        let caps = GraphqlCapabilities {
            queries: true,
            commands: true,
            live: true,
        };
        let delivery = if mode == "none" {
            None
        } else {
            Some(Arc::new(
                NativeDelivery::new(NativeDeliveryOptions {
                    snapshots: (mode == "snapshots").then_some(SnapshotLimits {
                        entries: 128,
                        bytes: 2 * 1024 * 1024,
                        entry_bytes: 256 * 1024,
                    }),
                    coalescing: (mode == "flights").then_some(FlightLimits {
                        groups: 8,
                        consumers: 128,
                        response_bytes: 256 * 1024,
                        ..Default::default()
                    }),
                    live: (mode == "live").then_some(LiveLimits {
                        groups: 4,
                        consumers: 128,
                        frame_bytes: 64 * 1024,
                        ..Default::default()
                    }),
                })
                .unwrap(),
            ))
        };
        let selected = DeliveryCapabilities {
            snapshots: mode == "snapshots",
            coalescing: mode == "flights",
            live_sharing: mode == "live",
        };
        let config = GatewayConfig {
            bindings: vec![Binding::new(
                "api",
                BindingKind::Graphql {
                    executor: GraphqlExecutor::Remote {
                        origin: format!("http://127.0.0.1:{meter_port}"),
                    },
                    capabilities: caps,
                    delivery: selected,
                    schema_extensions: vec![],
                },
            )],
            routes: vec![Route::new("api", RoutePath::prefix("/graphql"), "api")],
        }
        .build()
        .unwrap();
        let binding = GraphqlBinding::Remote(RemoteGraphql::default());
        let resource = delivery.clone().map_or_else(
            || NativeBinding::Graphql(binding.clone()),
            |d| NativeBinding::GraphqlWithDelivery(binding.clone(), d),
        );
        let gateway = NativeGateway::new(
            config,
            NativeOptions::new("http://load-gateway.invalid"),
            [("api".into(), resource)],
            NativeAuth::anonymous(),
        )
        .unwrap();
        let counts = delivery.clone();
        let reset = delivery;
        let router=gateway.router().route("/__coordinators",get(move || {let delivery=counts.clone();async move {axum::Json(delivery.map_or_else(||json!([]),|d|json!([{ "query":[0,d.flight_counts().0,d.flight_counts().1],"live":d.live_counts(),"metrics":d.metrics()}])))}}).post(move || {let delivery=reset.clone();async move {if let Some(d)=delivery{d.invalidate_all();}"reset"}}));
        let server = serve(router).await;
        native.insert(mode, server.origin.clone());
        servers.push(server);
    }
    let result = tokio::process::Command::new("node")
        .arg("tests/gateway-worker/load-runtime.mjs")
        .env("GATEWAY_ORIGIN", &origin.origin)
        .env("GATEWAY_METER_PORT", meter_port.to_string())
        .env(
            "GATEWAY_NATIVE_MODES",
            serde_json::to_string(&native).unwrap(),
        )
        .env("GATEWAY_TOKEN_ALICE", token("alice"))
        .env("GATEWAY_TOKEN_BOB", token("bob"))
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true)
        .output()
        .await
        .unwrap();
    println!("{}", String::from_utf8_lossy(&result.stdout));
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}
