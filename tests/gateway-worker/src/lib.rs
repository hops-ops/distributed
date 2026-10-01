use ::worker::wasm_bindgen;
use ::worker::{
    durable_object, event, Context, DurableObject, Env, Request, Response, Result, State,
};
use distributed::gateway::{delivery::*, worker::*, *};
use std::rc::Rc;
fn delivery_options(env: &Env) -> WorkerDeliveryOptions {
    let mode = env
        .var("DELIVERY_MODE")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| "all".into());
    if mode == "none" {
        return WorkerDeliveryOptions::default();
    }
    WorkerDeliveryOptions {
        snapshots: (mode == "all" || mode == "snapshots").then_some(SnapshotLimits {
            entries: 128,
            bytes: 2 * 1024 * 1024,
            entry_bytes: 256 * 1024,
        }),
        coalescing: (mode == "all" || mode == "flights").then_some(FlightLimits {
            groups: 8,
            consumers: 128,
            response_bytes: 256 * 1024,
            ..Default::default()
        }),
        live: (mode == "all" || mode == "live").then(|| LiveLimits {
            groups: 4,
            consumers: 128,
            frame_bytes: 64 * 1024,
            ..Default::default()
        }),
    }
}
fn gateway(env: &Env) -> Result<WorkerGateway> {
    let origin = env.var("UI_ORIGIN")?.to_string();
    let public = env.var("PUBLIC_ORIGIN")?.to_string();
    let api = env
        .var("API_ORIGIN")
        .map(|v| v.to_string())
        .unwrap_or_else(|_| origin.clone());
    let options = delivery_options(env);
    let config = GatewayConfig {
        bindings: vec![
            Binding::new("ui", BindingKind::UiProxy { origin }),
            Binding::new("health", BindingKind::Handler),
            Binding::new("owned", BindingKind::Handler),
            Binding::new(
                "api",
                BindingKind::Graphql {
                    executor: GraphqlExecutor::Remote { origin: api },
                    capabilities: GraphqlCapabilities {
                        queries: true,
                        commands: true,
                        live: true,
                    },
                    delivery: DeliveryCapabilities {
                        snapshots: options.snapshots.is_some(),
                        coalescing: options.coalescing.is_some(),
                        live_sharing: options.live.is_some(),
                    },
                    schema_extensions: vec![],
                },
            ),
        ],
        routes: vec![
            Route::new("health", RoutePath::exact("/__gateway_health"), "health"),
            Route::new("owned", RoutePath::prefix("/__owned"), "owned"),
            Route::new("api", RoutePath::prefix("/graphql"), "api"),
            Route::new("ui", RoutePath::prefix("/"), "ui"),
        ],
    }
    .build()
    .map_err(|error| ::worker::Error::RustError(error.to_string()))?;
    let mut worker_options = WorkerOptions::new(public);
    worker_options
        .strip_headers
        .push("x-distributed-subject".into());
    if let Ok(limit) = env.var("REQUEST_BYTES") {
        worker_options.limits.request_bytes =
            limit.to_string().parse().expect("fixture request limit");
    }
    WorkerGateway::new(
        config,
        worker_options,
        [
            ("ui".into(), WorkerBinding::UiProxy { websocket: true }),
            (
                "health".into(),
                WorkerBinding::Handler(WorkerHandler::new(|_, _, env| async move {
                    Response::ok(
                        env.var("INGRESS_ID")
                            .map(|v| v.to_string())
                            .unwrap_or_else(|_| "ready".into()),
                    )
                })),
            ),
            (
                "owned".into(),
                WorkerBinding::Handler(WorkerHandler::new(|_, _, _| async {
                    Response::error("owned error", 404)
                })),
            ),
            (
                "api".into(),
                WorkerBinding::Graphql {
                    http_path: "/graphql".into(),
                    live_path: Some("/graphql/ws".into()),
                    delivery: (options.snapshots.is_some()
                        || options.coalescing.is_some()
                        || options.live.is_some())
                    .then_some(WorkerDeliveryBinding {
                        namespace: "DELIVERY".into(),
                        epoch: "fixture-v1".into(),
                        shards: 4,
                        options,
                    }),
                },
            ),
        ],
        WorkerAuth::anonymous(),
    )
    .map_err(|error| ::worker::Error::RustError(error.to_string()))
}
#[durable_object]
pub struct DeliveryCoordinator {
    env: Env,
    coordinator: Rc<WorkerCoordinator>,
}
impl DurableObject for DeliveryCoordinator {
    fn new(_state: State, env: Env) -> Self {
        Self {
            coordinator: WorkerCoordinator::new(delivery_options(&env))
                .expect("validated fixture limits"),
            env,
        }
    }
    async fn fetch(&self, request: Request) -> Result<Response> {
        match request.path().as_str() {
            "/__metrics" => {
                return Response::from_json(
                    &serde_json::json!({"query":self.coordinator.counts(),"live":self.coordinator.live_counts(),"metrics":self.coordinator.metrics()}),
                )
            }
            "/__reset" => {
                self.coordinator.invalidate_all();
                return Response::ok("reset");
            }
            _ => {}
        }
        gateway(&self.env)?
            .fetch_coordinated(request, self.env.clone(), self.coordinator.clone())
            .await
    }
}
#[event(fetch)]
async fn fetch(request: Request, env: Env, _ctx: Context) -> Result<Response> {
    if request.path() == "/__coordinators" {
        let options = delivery_options(&env);
        if options.snapshots.is_none() && options.coalescing.is_none() && options.live.is_none() {
            return Response::from_json(&Vec::<serde_json::Value>::new());
        }
        let reset = request.method() == ::worker::Method::Post;
        let mut counts = Vec::new();
        for shard in 0..4 {
            let namespace = env.durable_object("DELIVERY")?;
            let stub = namespace
                .id_from_name(&format!("gateway-delivery-v1:fixture-v1:api:{shard}"))?
                .get_stub()?;
            let mut response = stub
                .fetch_with_str(&format!(
                    "http://internal.invalid/{}",
                    if reset { "__reset" } else { "__metrics" }
                ))
                .await?;
            if !reset {
                counts.push(response.json::<serde_json::Value>().await?);
            }
        }
        return Response::from_json(&counts);
    }
    gateway(&env)?.fetch(request, env).await
}
