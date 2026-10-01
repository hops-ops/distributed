//! Explicit workers-rs gateway mounting. No listener or domain projector is linked.
mod cancellation;
mod coordinator;
mod frontend;
mod live;
mod live_transport;
mod proxy;
mod raw_socket;
mod socket;
mod timer;
pub use coordinator::{WorkerCoordinator, WorkerDeliveryBinding, WorkerDeliveryOptions};

use super::{
    Admission, AuthError, BackendCredential, BindingKind, Credentials, Gateway, GatewayError,
    GraphqlExecutor, RequestContext,
};
use futures_util::future::LocalBoxFuture;
use std::{collections::BTreeMap, future::Future, rc::Rc};
use worker::{Env, Request, Response, Result};

/// Local-future provider; every consumer authenticates before route admission.
#[derive(Clone)]
pub struct WorkerAuth(
    Rc<
        dyn Fn(
            Credentials,
        ) -> LocalBoxFuture<'static, std::result::Result<RequestContext, AuthError>>,
    >,
);
impl WorkerAuth {
    /// Bind a trusted application/session provider. Backend authorization remains mandatory.
    pub fn new<F, Fut>(provider: F) -> Self
    where
        F: Fn(Credentials) -> Fut + 'static,
        Fut: Future<Output = std::result::Result<RequestContext, AuthError>> + 'static,
    {
        Self(Rc::new(move |credentials| Box::pin(provider(credentials))))
    }
    /// Opaque cookies may reach delegated auth/UI handlers. Unvalidated bearer input fails closed.
    pub fn anonymous() -> Self {
        Self::new(|credentials| async move {
            if credentials.authorization.is_some() {
                return Err(AuthError::Unauthorized);
            }
            RequestContext::from_provider(None, "anonymous-v1", BackendCredential::None)
                .map_err(|_| AuthError::Unavailable)
        })
    }
}
/// Custom mounted handler, including application-owned auth lifecycle handlers.
#[derive(Clone)]
pub struct WorkerHandler(
    Rc<dyn Fn(Request, RequestContext, Env) -> LocalBoxFuture<'static, Result<Response>>>,
);
impl WorkerHandler {
    /// Adapt a local Worker handler without requiring Send or a native runtime.
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(Request, RequestContext, Env) -> Fut + 'static,
        Fut: Future<Output = Result<Response>> + 'static,
    {
        Self(Rc::new(move |request, context, env| {
            Box::pin(handler(request, context, env))
        }))
    }
}
/// Explicit per-request and upgraded-connection bounds.
#[derive(Clone, Debug)]
pub struct WorkerLimits {
    /// Maximum incoming request bytes.
    pub request_bytes: usize,
    /// Maximum aggregate queued wire bytes per WebSocket (also bounds one frame).
    pub websocket_buffer_bytes: usize,
    /// Maximum response header wait in milliseconds.
    pub header_timeout_ms: u64,
    /// Maximum response stream idle wait in milliseconds.
    pub read_timeout_ms: u64,
    /// Maximum upgraded connection lifetime in milliseconds.
    pub websocket_lifetime_ms: u64,
}
impl Default for WorkerLimits {
    fn default() -> Self {
        Self {
            request_bytes: 256 * 1024,
            websocket_buffer_bytes: 256 * 1024,
            header_timeout_ms: 30000,
            read_timeout_ms: 60000,
            websocket_lifetime_ms: 3600000,
        }
    }
}
/// Trusted ingress configuration; public origin is never inferred from forwarded headers.
#[derive(Clone, Debug)]
pub struct WorkerOptions {
    /// Canonical public HTTP(S) origin.
    pub public_origin: String,
    /// Bounded transport settings.
    pub limits: WorkerLimits,
    /// Additional incoming identity/secret header names to remove.
    pub strip_headers: Vec<String>,
    /// Stable deployment identity used to reject proxy loops, not a secret.
    pub hop_id: String,
}
impl WorkerOptions {
    /// Default bounded settings for the public origin.
    pub fn new(public_origin: impl Into<String>) -> Self {
        Self {
            public_origin: public_origin.into(),
            limits: WorkerLimits::default(),
            strip_headers: Vec::new(),
            hop_id: "application-gateway-worker-v1".into(),
        }
    }
}
/// Resources allocated only for explicitly selected portable bindings.
#[derive(Clone)]
pub enum WorkerBinding {
    /// Local application handler.
    Handler(WorkerHandler),
    /// Static asset service binding, fetched only after route admission.
    Assets(String),
    /// Whole UI/auth reverse proxy.
    UiProxy {
        /// Allow transparent WebSocket upgrades to this UI target.
        websocket: bool,
    },
    /// Named portable admission policy.
    Admission(Admission),
    /// Whole remote GraphQL endpoint; embedded executors are rejected.
    Graphql {
        /// Origin HTTP path, independent of the public mount path.
        http_path: String,
        /// Optional origin live endpoint path.
        live_path: Option<String>,
        /// Optional sharded Durable Object delivery; never isolate-local state.
        delivery: Option<WorkerDeliveryBinding>,
    },
}
/// A mounted Worker gateway. Construction starts no network work.
#[derive(Clone)]
pub struct WorkerGateway(Rc<Inner>);
struct Inner {
    gateway: Gateway,
    options: WorkerOptions,
    bindings: BTreeMap<String, WorkerBinding>,
    auth: WorkerAuth,
}
impl WorkerGateway {
    /// Validate bindings and capabilities before handling requests.
    pub fn new(
        gateway: Gateway,
        options: WorkerOptions,
        bindings: impl IntoIterator<Item = (String, WorkerBinding)>,
        auth: WorkerAuth,
    ) -> std::result::Result<Self, GatewayError> {
        super::config::validate_origin(&options.public_origin)?;
        if options.hop_id.is_empty()
            || options.hop_id.len() > 128
            || !options
                .hop_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || options.strip_headers.len() > 128
            || options
                .strip_headers
                .iter()
                .any(|s| s.is_empty() || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'))
        {
            return Err(GatewayError("invalid Worker header configuration"));
        }
        let limits = &options.limits;
        if limits.request_bytes == 0
            || limits.request_bytes > 16 * 1024 * 1024
            || limits.websocket_buffer_bytes == 0
            || limits.websocket_buffer_bytes > 1024 * 1024
            || limits.header_timeout_ms == 0
            || limits.header_timeout_ms > 300000
            || limits.read_timeout_ms == 0
            || limits.read_timeout_ms > 300000
            || limits.websocket_lifetime_ms == 0
            || limits.websocket_lifetime_ms > 3600000
        {
            return Err(GatewayError("invalid Worker transport limits"));
        }
        let mut registry = BTreeMap::new();
        for (id, binding) in bindings {
            let declaration = gateway
                .binding(&id)
                .ok_or(GatewayError("undeclared Worker binding"))?;
            let compatible = match (&declaration.kind, &binding) {
                (BindingKind::Handler, WorkerBinding::Handler(_))
                | (BindingKind::Admission, WorkerBinding::Admission(_))
                | (BindingKind::Assets, WorkerBinding::Assets(_)) => true,
                (BindingKind::UiProxy { origin }, WorkerBinding::UiProxy { .. }) => {
                    origin.trim_end_matches('/') != options.public_origin.trim_end_matches('/')
                }
                (
                    BindingKind::Graphql {
                        executor: GraphqlExecutor::Remote { origin },
                        capabilities,
                        delivery,
                        ..
                    },
                    WorkerBinding::Graphql {
                        http_path,
                        live_path,
                        delivery: resource,
                    },
                ) => {
                    origin.trim_end_matches('/') != options.public_origin.trim_end_matches('/')
                        && super::route::normalize_path(http_path).is_ok()
                        && live_path
                            .as_ref()
                            .is_none_or(|p| super::route::normalize_path(p).is_ok())
                        && capabilities.live == live_path.is_some()
                        && resource.as_ref().map_or_else(
                            || {
                                !delivery.snapshots
                                    && !delivery.coalescing
                                    && !delivery.live_sharing
                            },
                            |resource| {
                                resource.validate().is_ok()
                                    && resource.options.capabilities() == *delivery
                            },
                        )
                }
                _ => false,
            };
            if !compatible || registry.insert(id, binding).is_some() {
                return Err(GatewayError("incompatible or duplicate Worker binding"));
            }
        }
        if gateway.routes().iter().any(|route| {
            !registry.contains_key(&route.target)
                || route.admission.iter().any(|id| !registry.contains_key(id))
        }) {
            return Err(GatewayError("missing Worker binding"));
        }
        Ok(Self(Rc::new(Inner {
            gateway,
            options,
            bindings: registry,
            auth,
        })))
    }
    /// Select one owner, authenticate, admit and execute exactly once.
    pub async fn fetch(&self, request: Request, env: Env) -> Result<Response> {
        cancellation::run(request.inner().signal(), self.dispatch(request, env, None)).await
    }
    /// Execute inside the selected Durable Object, repeating provider and origin admission.
    /// This entrypoint is an in-process mount, never a client header or URL flag.
    pub async fn fetch_coordinated(
        &self,
        request: Request,
        env: Env,
        coordinator: Rc<WorkerCoordinator>,
    ) -> Result<Response> {
        cancellation::run(
            request.inner().signal(),
            self.dispatch(request, env, Some(coordinator)),
        )
        .await
    }
    async fn dispatch(
        &self,
        mut request: Request,
        env: Env,
        coordinator: Option<Rc<WorkerCoordinator>>,
    ) -> Result<Response> {
        let url = request.url()?;
        let selected = match self.0.gateway.select(request.method().as_ref(), url.path()) {
            Ok(Some(selected)) => selected,
            Ok(None) => return Response::error("not found", 404),
            Err(_) => return Response::error("invalid route", 400),
        };
        let credentials = Credentials {
            authorization: request.headers().get("authorization")?,
            cookie: request.headers().get("cookie")?,
        };
        let context = match (self.0.auth.0)(credentials).await {
            Ok(context) => context,
            Err(error) => return auth_error(error),
        };
        if let Err(error) = Admission::Public.check(&context, now()) {
            return auth_error(error);
        }
        for id in &selected.route().admission {
            let Some(WorkerBinding::Admission(policy)) = self.0.bindings.get(id) else {
                return Response::error("invalid admission", 503);
            };
            if let Err(error) = policy.check(&context, now()) {
                return auth_error(error);
            }
        }
        if !selected.method_allowed() {
            return Response::error("method not allowed", 405);
        }
        let binding = &self.0.bindings[&selected.binding().id];
        match (binding, &selected.binding().kind) {
            (WorkerBinding::Handler(handler), _) => (handler.0)(request, context, env).await,
            (WorkerBinding::Assets(binding), _) => {
                if !matches!(request.method(), worker::Method::Get | worker::Method::Head) {
                    return Response::error("method not allowed", 405);
                }
                env.service(binding)?.fetch_request(request).await
            }
            (WorkerBinding::UiProxy { websocket }, BindingKind::UiProxy { origin }) => {
                proxy::forward(request, origin, None, *websocket, &self.0.options, &context).await
            }
            (
                WorkerBinding::Graphql {
                    http_path,
                    live_path,
                    delivery,
                },
                BindingKind::Graphql {
                    executor: GraphqlExecutor::Remote { origin },
                    capabilities,
                    ..
                },
            ) => {
                if proxy::is_upgrade(&request)? {
                    if let (Some(coordinator), Some(delivery)) = (&coordinator, delivery) {
                        if coordinator.capabilities() != delivery.options.capabilities() {
                            return Response::error("coordinator configuration mismatch", 503);
                        }
                        let input = coordinator::OriginRequest {
                            origin: origin.clone(),
                            path: http_path.clone(),
                            url: request.url()?.to_string(),
                            headers: request.headers().entries().collect(),
                            options: self.0.options.clone(),
                            context,
                        };
                        return coordinator.upgrade(
                            input,
                            live_path
                                .clone()
                                .ok_or_else(|| worker::Error::RustError("live disabled".into()))?,
                            *capabilities,
                        );
                    }
                }
                if proxy::is_upgrade(&request)? && coordinator.is_none() {
                    if !capabilities.live {
                        return Response::error("live disabled", 405);
                    }
                    let input = coordinator::OriginRequest {
                        origin: origin.clone(),
                        path: http_path.clone(),
                        url: request.url()?.to_string(),
                        headers: request.headers().entries().collect(),
                        options: self.0.options.clone(),
                        context,
                    };
                    return frontend::upgrade(
                        self.clone(),
                        env,
                        request,
                        input,
                        live_path.clone().expect("validated live path"),
                        *capabilities,
                        delivery.clone(),
                        selected.binding().id.clone(),
                    )
                    .await;
                }
                if request.method() != worker::Method::Post {
                    return Response::error("method not allowed", 405);
                }
                let bytes = match proxy::timeout(
                    self.0.options.limits.header_timeout_ms,
                    proxy::read_request(&mut request, self.0.options.limits.request_bytes),
                )
                .await
                {
                    Ok(bytes) => bytes,
                    Err(worker::Error::RustError(message)) if message == "request too large" => {
                        return Response::error(message, 413)
                    }
                    Err(_) => return Response::error("invalid or timed out request body", 400),
                };
                let value: serde_json::Value = match serde_json::from_slice(&bytes) {
                    Ok(value) => value,
                    Err(_) => return Response::error("invalid GraphQL request", 400),
                };
                if let Err(error) = super::graphql::admit_request(&value, *capabilities) {
                    return Response::from_json(&error.envelope());
                }
                if let Some(delivery) = delivery.as_ref().filter(|delivery| {
                    delivery.options.snapshots.is_some() || delivery.options.coalescing.is_some()
                }) {
                    if super::graphql::operation_kind(
                        value["query"].as_str().unwrap_or(""),
                        value["operationName"].as_str(),
                    ) == Ok(super::graphql::OperationKind::Query)
                    {
                        if let Some(coordinator) = coordinator {
                            if coordinator.capabilities() != delivery.options.capabilities() {
                                return Response::error("coordinator configuration mismatch", 503);
                            }
                            let input = coordinator::OriginRequest {
                                origin: origin.clone(),
                                path: http_path.clone(),
                                url: request.url()?.to_string(),
                                headers: request.headers().entries().collect(),
                                options: self.0.options.clone(),
                                context,
                            };
                            return coordinator.execute(input, value).await;
                        }
                        let shard = delivery.shard(&selected.binding().id, &value)?;
                        let namespace = env.durable_object(&delivery.namespace)?;
                        let stub = namespace.id_from_name(&shard)?.get_stub()?;
                        return stub
                            .fetch_with_request(proxy::with_body(&request, bytes)?)
                            .await;
                    }
                }
                let request = proxy::with_body(&request, bytes)?;
                proxy::forward(
                    request,
                    origin,
                    Some(http_path),
                    false,
                    &self.0.options,
                    &context,
                )
                .await
            }
            _ => Response::error("invalid execution binding", 503),
        }
    }
}
fn auth_error(error: AuthError) -> Result<Response> {
    Response::error(
        error.to_string(),
        match error {
            AuthError::Unauthorized => 401,
            AuthError::Forbidden => 403,
            AuthError::Unavailable => 503,
        },
    )
}
fn now() -> u64 {
    worker::Date::now().as_millis() / 1000
}
