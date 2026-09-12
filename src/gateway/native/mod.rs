//! Opt-in native HTTP gateway. Construction starts no listener or projector.
mod assets;
#[cfg(feature = "gateway-graphql-native")]
mod graphql;
mod proxy;
#[cfg(feature = "gateway-graphql-native")]
pub use graphql::{EmbeddedGraphql, GraphqlBinding, RemoteGraphql};

use super::{
    Admission, AuthError, BackendCredential, BindingKind, Credentials, Gateway, GatewayAdapter,
    GatewayError, Rejection, RequestContext, SelectedRoute,
};
pub use assets::{Asset, StaticAssets};
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, HeaderName, HeaderValue, Request, StatusCode},
    response::Response,
    Router,
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;
use tower::ServiceExt;
use url::Url;

/// Boxed native provider result. Providers receive credentials, never identity
/// headers. Use one shared validator/session provider for all route owners.
type AuthFuture = Pin<Box<dyn Future<Output = Result<RequestContext, AuthError>> + Send>>;

/// Native provider registry entry. Worker adapters can use the local-future
/// portable AuthProvider trait without this Send/Sync runtime requirement.
#[derive(Clone)]
pub struct NativeAuth(Arc<dyn Fn(Credentials) -> AuthFuture + Send + Sync>);
impl NativeAuth {
    /// Adapt a configured native provider; the closure runs on every request.
    pub fn new<F, Fut>(provider: F) -> Self
    where
        F: Fn(Credentials) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<RequestContext, AuthError>> + Send + 'static,
    {
        Self(Arc::new(move |credentials| Box::pin(provider(credentials))))
    }
    /// Gateway without identity admission. Cookies remain opaque for delegated
    /// UI/auth handlers; bearer credentials fail closed rather than being trusted.
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

/// Explicit proxy resource limits, shared by all mounted targets.
#[derive(Clone, Debug)]
pub struct ProxyLimits {
    /// Maximum incoming body size. Known oversize bodies fail before forwarding.
    pub request_body_bytes: usize,
    /// Maximum active HTTP streams and upgraded connections; overload gets 503.
    pub concurrent_requests: usize,
    /// Connect and response-header wait bound.
    pub response_header_timeout: Duration,
    /// Idle time between response reads; no whole-response buffering.
    pub read_timeout: Duration,
    /// Maximum upgraded-connection lifetime, also capped by identity expiry.
    pub upgrade_lifetime: Duration,
}
impl Default for ProxyLimits {
    fn default() -> Self {
        Self {
            request_body_bytes: 16 * 1024 * 1024,
            concurrent_requests: 1024,
            response_header_timeout: Duration::from_secs(30),
            read_timeout: Duration::from_secs(60),
            upgrade_lifetime: Duration::from_secs(3600),
        }
    }
}

/// Host configuration. The public origin is trusted configuration, not Host or
/// forwarded headers from a request. Upstreams cannot point back to this origin.
#[derive(Clone, Debug)]
pub struct NativeOptions {
    /// Absolute HTTP(S) public origin, without path or credentials.
    pub public_origin: String,
    /// Resource limits applied before/throughout forwarding.
    pub limits: ProxyLimits,
    /// Deployment-specific incoming identity/secret names to strip in addition
    /// to the framework denylist. Values never appear in configuration errors.
    pub strip_headers: Vec<String>,
}
impl NativeOptions {
    /// Default bounded proxy settings for this public origin.
    pub fn new(public_origin: impl Into<String>) -> Self {
        Self {
            public_origin: public_origin.into(),
            limits: ProxyLimits::default(),
            strip_headers: Vec::new(),
        }
    }
}

/// Runtime resource for one portable binding. Duplicate or incompatible binding
/// entries are rejected before serving. Application handlers receive admitted
/// RequestContext in request extensions; backend authorization stays theirs.
#[derive(Clone)]
pub enum NativeBinding {
    /// Complete application router; URI is preserved, without prefix stripping.
    Handler(Router),
    /// Embedded or complete remote GraphQL executor.
    #[cfg(feature = "gateway-graphql-native")]
    Graphql(GraphqlBinding),
    /// GraphQL with an explicitly allocated bounded delivery coordinator.
    #[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
    GraphqlWithDelivery(GraphqlBinding, Arc<NativeDelivery>),
    /// Configured UI proxy. Upgrades are disabled unless explicitly selected.
    UiProxy {
        /// Whether this target may negotiate a WebSocket upgrade.
        websocket: bool,
    },
    /// Bounded, preloaded static inventory; no request-selected filesystem I/O.
    Assets(StaticAssets),
    /// Named route admission policy.
    Admission(Admission),
}

/// Validated native adapter, shareable between explicit application mounts.
#[derive(Clone)]
pub struct NativeGateway(Arc<NativeInner>);
struct NativeInner {
    gateway: Gateway,
    options: NativeOptions,
    origin: Url,
    bindings: BTreeMap<String, NativeBinding>,
    auth: NativeAuth,
    client: reqwest::Client,
    permits: Arc<Semaphore>,
    hop_id: String,
}

impl NativeGateway {
    /// Validate runtime bindings and construct only the selected adapter. Caller
    /// owns the listener/process lifecycle; this method opens no connections.
    pub fn new(
        gateway: Gateway,
        options: NativeOptions,
        bindings: impl IntoIterator<Item = (String, NativeBinding)>,
        auth: NativeAuth,
    ) -> Result<Self, GatewayError> {
        super::config::validate_origin(&options.public_origin)?;
        let origin = Url::parse(&options.public_origin)
            .map_err(|_| GatewayError("invalid public origin"))?;
        let limits = &options.limits;
        if limits.request_body_bytes == 0
            || limits.concurrent_requests == 0
            || limits.concurrent_requests > 65536
            || limits.response_header_timeout.is_zero()
            || limits.read_timeout.is_zero()
            || limits.upgrade_lifetime.is_zero()
        {
            return Err(GatewayError("invalid native proxy limits"));
        }
        if options.strip_headers.len() > 128
            || options
                .strip_headers
                .iter()
                .any(|name| HeaderName::from_bytes(name.as_bytes()).is_err())
        {
            return Err(GatewayError("invalid identity header strip list"));
        }
        let mut registry = BTreeMap::new();
        for (id, binding) in bindings {
            let declaration = gateway
                .binding(&id)
                .ok_or(GatewayError("undeclared native binding"))?;
            #[allow(unused_mut)]
            let mut compatible = matches!(
                (&declaration.kind, &binding),
                (BindingKind::Handler, NativeBinding::Handler(_))
                    | (BindingKind::UiProxy { .. }, NativeBinding::UiProxy { .. })
                    | (BindingKind::Assets, NativeBinding::Assets(_))
                    | (BindingKind::Admission, NativeBinding::Admission(_))
            );
            #[cfg(feature = "gateway-graphql-native")]
            if let (
                BindingKind::Graphql {
                    executor,
                    capabilities,
                    delivery,
                    schema_extensions,
                },
                NativeBinding::Graphql(binding),
            ) = (&declaration.kind, &binding)
            {
                binding.validate(
                    executor,
                    *capabilities,
                    *delivery,
                    schema_extensions,
                    &origin,
                )?;
                compatible = true;
            }
            #[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
            if let (
                BindingKind::Graphql {
                    executor,
                    capabilities,
                    delivery,
                    schema_extensions,
                },
                NativeBinding::GraphqlWithDelivery(binding, coordinator),
            ) = (&declaration.kind, &binding)
            {
                if *delivery != coordinator.capabilities() {
                    return Err(GatewayError("delivery resource does not match declaration"));
                }
                binding.validate(
                    executor,
                    *capabilities,
                    super::DeliveryCapabilities::default(),
                    schema_extensions,
                    &origin,
                )?;
                compatible = true;
            }
            if !compatible {
                return Err(GatewayError("incompatible native binding"));
            }
            if let BindingKind::UiProxy { origin: upstream } = &declaration.kind {
                let upstream =
                    Url::parse(upstream).map_err(|_| GatewayError("invalid upstream"))?;
                if upstream.origin() == origin.origin() {
                    return Err(GatewayError("upstream points to public gateway"));
                }
            }
            if registry.insert(id, binding).is_some() {
                return Err(GatewayError("duplicate native binding"));
            }
        }
        for route in gateway.routes() {
            for id in std::iter::once(&route.target).chain(&route.admission) {
                if !registry.contains_key(id) {
                    return Err(GatewayError("missing native runtime binding"));
                }
            }
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .connect_timeout(limits.response_header_timeout)
            .read_timeout(limits.read_timeout)
            .build()
            .map_err(|_| GatewayError("cannot construct proxy client"))?;
        let permits = Arc::new(Semaphore::new(limits.concurrent_requests));
        Ok(Self(Arc::new(NativeInner {
            gateway,
            options,
            origin,
            bindings: registry,
            auth,
            client,
            permits,
            hop_id: uuid::Uuid::now_v7().to_string(),
        })))
    }

    /// Mount on the application's HTTP server. Every path goes through portable
    /// selection/admission before an application handler or proxy executes.
    pub fn router(self) -> Router {
        Router::new().fallback(Self::handle).with_state(self)
    }

    async fn handle(State(this): State<Self>, request: Request<Body>) -> Response {
        if let Some(hops) = request.headers().get("x-distributed-gateway-hops") {
            let Ok(hops) = hops.to_str() else {
                return response(StatusCode::BAD_REQUEST);
            };
            if hops.len() > 512
                || hops.split(',').count() >= 8
                || hops.split(',').any(|hop| hop.trim() == this.0.hop_id)
            {
                return response(StatusCode::LOOP_DETECTED);
            }
        }
        this.0.gateway.dispatch(&this, request).await
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(u64::MAX, |d| d.as_secs())
}
fn response(status: StatusCode) -> Response {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .expect("static response")
}
fn auth_response(error: AuthError) -> Response {
    response(match error {
        AuthError::Unauthorized => StatusCode::UNAUTHORIZED,
        AuthError::Forbidden => StatusCode::FORBIDDEN,
        AuthError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
    })
}

fn credentials(headers: &HeaderMap) -> Result<Credentials, AuthError> {
    let mut authorization = headers.get_all(header::AUTHORIZATION).iter();
    let authorization_value = authorization
        .next()
        .map(|v| v.to_str().map(str::to_owned))
        .transpose()
        .map_err(|_| AuthError::Unauthorized)?;
    if authorization.next().is_some() {
        return Err(AuthError::Unauthorized);
    }
    let cookies = headers
        .get_all(header::COOKIE)
        .iter()
        .map(|v| v.to_str())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| AuthError::Unauthorized)?;
    if authorization_value.as_ref().map_or(0, String::len)
        + cookies.iter().map(|v| v.len()).sum::<usize>()
        > 32768
    {
        return Err(AuthError::Unauthorized);
    }
    Ok(Credentials {
        authorization: authorization_value,
        cookie: if cookies.is_empty() {
            None
        } else {
            Some(cookies.join("; "))
        },
    })
}

impl GatewayAdapter for NativeGateway {
    type Request = Request<Body>;
    type Context = RequestContext;
    type Response = Response;
    fn method<'a>(&self, request: &'a Self::Request) -> &'a str {
        request.method().as_str()
    }
    fn target<'a>(&self, request: &'a Self::Request) -> &'a str {
        request.uri().path_and_query().map_or("/", |p| p.as_str())
    }
    fn admit(
        &self,
        selected: SelectedRoute<'_>,
        request: &Self::Request,
    ) -> impl Future<Output = Result<RequestContext, Response>> {
        // Own credential metadata before awaiting. A streaming HTTP Body is
        // Send but not Sync, so never retain &Request across provider I/O.
        let credentials = credentials(request.headers());
        let auth = self.0.auth.clone();
        let policies: Vec<_> = selected
            .route()
            .admission
            .iter()
            .filter_map(|name| match self.0.bindings.get(name) {
                Some(NativeBinding::Admission(policy)) => Some(policy.clone()),
                _ => None,
            })
            .collect();
        async move {
            let context = (auth.0)(credentials.map_err(auth_response)?)
                .await
                .map_err(auth_response)?;
            Admission::Public
                .check(&context, now())
                .map_err(auth_response)?;
            for policy in policies {
                policy.check(&context, now()).map_err(auth_response)?;
            }
            Ok(context)
        }
    }
    async fn execute(
        &self,
        selected: SelectedRoute<'_>,
        context: RequestContext,
        mut request: Self::Request,
    ) -> Response {
        let Some(binding) = self.0.bindings.get(&selected.binding().id) else {
            return response(StatusCode::SERVICE_UNAVAILABLE);
        };
        match binding {
            NativeBinding::Handler(handler) => {
                if proxy::prepare_headers(request.headers_mut(), &self.0, &context, false).is_err()
                {
                    return response(StatusCode::BAD_REQUEST);
                }
                request.extensions_mut().insert(context);
                match handler.clone().oneshot(request).await {
                    Ok(response) => response,
                    Err(never) => match never {},
                }
            }
            NativeBinding::UiProxy { websocket } => {
                let BindingKind::UiProxy { origin } = &selected.binding().kind else {
                    return response(StatusCode::SERVICE_UNAVAILABLE);
                };
                proxy::forward(&self.0, origin, *websocket, context, request).await
            }
            #[cfg(feature = "gateway-graphql-native")]
            NativeBinding::Graphql(binding) => {
                binding
                    .execute(&self.0, &selected.binding().kind, context, request)
                    .await
            }
            #[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
            NativeBinding::GraphqlWithDelivery(binding, coordinator) => {
                request.extensions_mut().insert(Arc::clone(coordinator));
                binding
                    .execute(&self.0, &selected.binding().kind, context, request)
                    .await
            }
            NativeBinding::Assets(assets) => assets.serve(request),
            NativeBinding::Admission(_) => response(StatusCode::SERVICE_UNAVAILABLE),
        }
    }
    fn reject(&self, rejection: Rejection<'_>) -> Response {
        match rejection {
            Rejection::BadRequest => response(StatusCode::BAD_REQUEST),
            Rejection::NotFound => response(StatusCode::NOT_FOUND),
            Rejection::MethodNotAllowed(selected) => {
                let mut response = response(StatusCode::METHOD_NOT_ALLOWED);
                if let super::Methods::Only(methods) = &selected.route().methods {
                    if let Ok(allow) = HeaderValue::from_str(&methods.join(", ")) {
                        response.headers_mut().insert(header::ALLOW, allow);
                    }
                }
                response
            }
        }
    }
}

#[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
mod delivery;
#[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
pub use delivery::{NativeDelivery, NativeDeliveryOptions};

#[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
mod flight;

#[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
mod live;

#[cfg(all(feature = "gateway-graphql-native", feature = "gateway-delivery"))]
mod live_transport;
