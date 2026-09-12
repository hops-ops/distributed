use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use super::route::{normalize_path, Route, RoutePath, SelectedRoute};

/// Maximum declared routes in one gateway configuration.
pub const MAX_ROUTES: usize = 256;
/// Maximum runtime bindings in one gateway configuration.
pub const MAX_BINDINGS: usize = 256;
/// Maximum bytes in route, binding, policy and schema-extension identifiers.
pub const MAX_ID_BYTES: usize = 256;

/// Invalid gateway configuration or request metadata. Errors never echo URLs,
/// headers or credentials supplied by a caller.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayError(pub(crate) &'static str);

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for GatewayError {}

pub(crate) fn validate_id(id: &str) -> Result<(), GatewayError> {
    if id.len() > MAX_ID_BYTES {
        return Err(GatewayError("gateway identifier exceeds size bound"));
    }
    crate::application::LogicalId::try_new("gateway identifier", id)
        .map(|_| ())
        .map_err(|_| GatewayError("invalid gateway identifier"))
}

/// Optional optimizations. These flags select adapter capabilities, never
/// authorize reuse. Origin-validated identity and freshness are still required.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryCapabilities {
    /// Complete query snapshot reuse with origin validation.
    pub snapshots: bool,
    /// Equivalent concurrent query execution sharing.
    pub coalescing: bool,
    /// Equivalent upstream live subscription sharing.
    pub live_sharing: bool,
}

/// Explicit GraphQL surface selection at the bound executor.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GraphqlCapabilities {
    /// Expose command mutations.
    pub commands: bool,
    /// Expose queries.
    pub queries: bool,
    /// Expose live operations.
    pub live: bool,
}

/// Location of the composed executor. Remote requests remain whole operations.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum GraphqlExecutor {
    /// Adapter-owned executor in this process.
    Embedded,
    /// Complete remote executor at this configured HTTP(S) origin. The adapter
    /// owns endpoint-path configuration and credential trust.
    Remote {
        /// Absolute origin without userinfo, path, query or fragment.
        origin: String,
    },
}

/// Kind of adapter resource explicitly selected by configuration. No handles,
/// secrets, native futures or database pools are part of this declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BindingKind {
    /// Application HTTP handler or delegated auth lifecycle handler.
    Handler,
    /// Authentication/admission policy implemented by the selected adapter.
    Admission,
    /// Native or platform assets; route admission precedes serving.
    Assets,
    /// UI server at a configured origin; never a request-selected proxy URL.
    UiProxy {
        /// Absolute HTTP(S) origin without userinfo, path, query or fragment.
        origin: String,
    },
    /// GraphQL executor and its explicitly selected capabilities.
    Graphql {
        /// Local or complete remote execution.
        executor: GraphqlExecutor,
        /// Surface components implemented by the executor.
        capabilities: GraphqlCapabilities,
        /// Independent optional delivery mounts; all disabled by default.
        delivery: DeliveryCapabilities,
        /// Adapter registration identifiers for local schema extensions.
        /// Remote schemas must install their own fields at the remote executor;
        /// declaring local extensions with a remote binding is rejected.
        schema_extensions: Vec<String>,
    },
}

/// Named adapter binding. Identifiers are shared with route targets/admission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// Unique configuration identifier.
    pub id: String,
    /// Required adapter capability.
    pub kind: BindingKind,
}

impl Binding {
    /// Declare a binding. [`GatewayConfig::build`] validates all declarations.
    pub fn new(id: impl Into<String>, kind: BindingKind) -> Self {
        Self {
            id: id.into(),
            kind,
        }
    }
}

/// Portable configuration, validated atomically before a gateway is usable.
/// Declaration order does not change routing. Deserialization alone does not
/// validate references or grant authority; always call [`Self::build`].
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    /// Explicit resource declarations. Merely linking an adapter adds nothing.
    pub bindings: Vec<Binding>,
    /// Explicit path ownership and admission chains.
    pub routes: Vec<Route>,
}

/// Validated immutable route and binding inventory. Creating this value performs
/// no I/O or runtime allocation other than its bounded configuration storage.
#[derive(Clone, Debug)]
pub struct Gateway {
    routes: Vec<Route>,
    bindings: BTreeMap<String, Binding>,
}

impl GatewayConfig {
    /// Validate declarations, references, bounds and deterministic ownership.
    ///
    /// # Errors
    /// Rejects duplicate/ambiguous paths or IDs, missing/wrong-kind bindings,
    /// invalid origins, malformed paths and incompatible capability selections.
    pub fn build(self) -> Result<Gateway, GatewayError> {
        if self.routes.len() > MAX_ROUTES || self.bindings.len() > MAX_BINDINGS {
            return Err(GatewayError(
                "gateway configuration exceeds inventory bounds",
            ));
        }
        let mut bindings = BTreeMap::new();
        for binding in self.bindings {
            validate_id(&binding.id)?;
            validate_binding(&binding.kind)?;
            if bindings.insert(binding.id.clone(), binding).is_some() {
                return Err(GatewayError("duplicate gateway binding identifier"));
            }
        }
        let mut route_ids = BTreeSet::new();
        let mut paths = BTreeSet::new();
        let mut routes = self.routes;
        for route in &mut routes {
            route.validate()?;
            if !route_ids.insert(route.id.clone()) {
                return Err(GatewayError("duplicate gateway route identifier"));
            }
            if !paths.insert(route.path.clone()) {
                return Err(GatewayError("duplicate gateway path owner"));
            }
            let target = bindings
                .get(&route.target)
                .ok_or(GatewayError("gateway route references a missing binding"))?;
            if matches!(target.kind, BindingKind::Admission) {
                return Err(GatewayError("admission binding cannot execute a route"));
            }
            for policy in &route.admission {
                if !matches!(
                    bindings.get(policy).map(|b| &b.kind),
                    Some(BindingKind::Admission)
                ) {
                    return Err(GatewayError(
                        "route admission requires an admission binding",
                    ));
                }
            }
        }
        // Stable declaration order aids adapters and diagnostics. Selection below
        // ranks exact paths and prefix length rather than registration order.
        routes.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Gateway { routes, bindings })
    }
}

impl Gateway {
    /// The validated route inventory, ordered by identifier.
    pub fn routes(&self) -> &[Route] {
        &self.routes
    }

    /// Deterministic logical binding inventory for explicit application mounts.
    pub fn bindings(&self) -> impl ExactSizeIterator<Item = &Binding> {
        self.bindings.values()
    }

    /// Resolve a configured resource without accepting a caller-selected URL.
    pub fn binding(&self, id: &str) -> Option<&Binding> {
        self.bindings.get(id)
    }

    /// Resolve path ownership before checking the HTTP method. An unsupported
    /// method stays owned by its selected route; it never falls through to UI.
    /// Prefixes match at segment boundaries. Percent-encoded path aliases are
    /// decoded once; encoded separators and ambiguous traversal are rejected.
    ///
    /// # Errors
    /// Rejects malformed methods/targets and bounded or ambiguous path input.
    pub fn select(
        &self,
        method: &str,
        target: &str,
    ) -> Result<Option<SelectedRoute<'_>>, GatewayError> {
        super::route::validate_method(method)?;
        if target.len() > 16 * 1024
            || target.contains('#')
            || target.bytes().any(|b| b.is_ascii_control())
        {
            return Err(GatewayError("invalid gateway request target"));
        }
        let path = normalize_path(target.split('?').next().unwrap_or_default())?;
        let route = self
            .routes
            .iter()
            .filter(|r| r.path.matches(&path))
            .max_by_key(|r| match &r.path {
                RoutePath::Exact(p) => (true, p.len()),
                RoutePath::Prefix(p) => (false, p.len()),
            });
        Ok(route.map(|route| SelectedRoute {
            route,
            binding: &self.bindings[&route.target],
            method_allowed: route.methods.allows(method),
        }))
    }
}

fn validate_binding(kind: &BindingKind) -> Result<(), GatewayError> {
    match kind {
        BindingKind::UiProxy { origin } => validate_origin(origin)?,
        BindingKind::Graphql {
            executor,
            capabilities,
            delivery,
            schema_extensions,
        } => {
            if !capabilities.commands && !capabilities.queries && !capabilities.live {
                return Err(GatewayError(
                    "GraphQL binding must select a surface capability",
                ));
            }
            if ((delivery.snapshots || delivery.coalescing) && !capabilities.queries)
                || (delivery.live_sharing && !capabilities.live)
            {
                return Err(GatewayError(
                    "delivery mount requires its query or live capability",
                ));
            }
            if let GraphqlExecutor::Remote { origin } = executor {
                validate_origin(origin)?;
                if !schema_extensions.is_empty() {
                    return Err(GatewayError(
                        "remote schema extensions must be registered at the remote executor",
                    ));
                }
            }
            if schema_extensions.len() > MAX_BINDINGS {
                return Err(GatewayError("too many schema extensions"));
            }
            let mut seen = BTreeSet::new();
            for extension in schema_extensions {
                validate_id(extension)?;
                if !seen.insert(extension) {
                    return Err(GatewayError("duplicate schema extension identifier"));
                }
            }
        }
        BindingKind::Handler | BindingKind::Admission | BindingKind::Assets => {}
    }
    Ok(())
}

pub(crate) fn validate_origin(origin: &str) -> Result<(), GatewayError> {
    let invalid = GatewayError(
        "binding requires an absolute HTTP(S) origin without credentials, path, query or fragment",
    );
    if origin.len() > 2048
        || origin
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control() || b == b'\\')
    {
        return Err(invalid);
    }
    let parsed = url::Url::parse(origin).map_err(|_| invalid.clone())?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || parsed.path() != "/"
    {
        return Err(invalid);
    }
    // Check the original authority/path as well: URL parsing can erase dot
    // segments or repair missing slashes, neither is an origin declaration.
    let authority = origin.split_once("://").ok_or(invalid.clone())?.1;
    if authority.trim_end_matches('/').contains('/')
        || authority.contains('@')
        || authority.ends_with("//")
    {
        return Err(invalid);
    }
    Ok(())
}
