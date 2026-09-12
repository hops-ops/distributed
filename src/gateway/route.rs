use super::config::{validate_id, Binding, GatewayError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Maximum UTF-8 bytes in a route or request path before decoding.
pub const MAX_PATH_BYTES: usize = 4096;
/// Maximum ordered admission policies on one route.
pub const MAX_ADMISSIONS: usize = 16;

/// Explicit ownership of an exact path or a segment-delimited prefix.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(
    tag = "kind",
    content = "path",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum RoutePath {
    /// Matches just this path, before every prefix match.
    Exact(String),
    /// Matches this path and descendants; `/` is the explicit UI fallback.
    Prefix(String),
}

impl RoutePath {
    /// Declare an exact route. Validation happens during configuration build.
    pub fn exact(path: impl Into<String>) -> Self {
        Self::Exact(path.into())
    }
    /// Declare a prefix. A trailing slash is normalized during build.
    pub fn prefix(path: impl Into<String>) -> Self {
        Self::Prefix(path.into())
    }
    pub(crate) fn matches(&self, path: &str) -> bool {
        match self {
            Self::Exact(value) => value == path,
            Self::Prefix(value) => {
                value == "/"
                    || value == path
                    || path
                        .strip_prefix(value)
                        .is_some_and(|tail| tail.starts_with('/'))
            }
        }
    }
}

/// HTTP method selection within one path owner. Disjoint method registrations
/// at the same path are rejected too: use one owner with an explicit list.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "methods",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Methods {
    /// Delegate all valid HTTP methods, including HEAD/OPTIONS, to this owner.
    #[default]
    Any,
    /// Exactly these case-sensitive methods. HEAD is not implicitly GET.
    Only(Vec<String>),
}

impl Methods {
    /// Whether this selection admits the supplied method.
    pub fn allows(&self, method: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Only(methods) => methods.iter().any(|m| m == method),
        }
    }
}

/// One route declaration. Public routes have an empty admission chain; protected
/// assets use the same ordered policies as API/custom routes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    /// Unique route identifier.
    pub id: String,
    /// Owned exact path or prefix.
    pub path: RoutePath,
    /// Configured execution binding identifier.
    pub target: String,
    /// Supported HTTP methods within this owner.
    pub methods: Methods,
    /// Ordered admission binding identifiers, all required before execution.
    pub admission: Vec<String>,
}

impl Route {
    /// Declare a public route accepting all methods. Add policies/methods
    /// explicitly before building the configuration.
    pub fn new(id: impl Into<String>, path: RoutePath, target: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            path,
            target: target.into(),
            methods: Methods::Any,
            admission: Vec::new(),
        }
    }

    pub(crate) fn validate(&mut self) -> Result<(), GatewayError> {
        validate_id(&self.id)?;
        validate_id(&self.target)?;
        match &mut self.path {
            RoutePath::Exact(path) => *path = normalize_path(path)?,
            RoutePath::Prefix(path) => {
                *path = normalize_path(path)?;
                if path.len() > 1 {
                    *path = path.trim_end_matches('/').to_owned();
                }
            }
        }
        if let Methods::Only(methods) = &mut self.methods {
            if methods.is_empty() || methods.len() > 32 {
                return Err(GatewayError("method selection must contain 1..=32 methods"));
            }
            let mut seen = BTreeSet::new();
            for method in methods.iter() {
                validate_method(method)?;
                if !seen.insert(method) {
                    return Err(GatewayError("duplicate route method"));
                }
            }
            methods.sort();
        }
        if self.admission.len() > MAX_ADMISSIONS {
            return Err(GatewayError("too many route admission policies"));
        }
        let mut seen = BTreeSet::new();
        for policy in &self.admission {
            validate_id(policy)?;
            if !seen.insert(policy) {
                return Err(GatewayError("duplicate route admission policy"));
            }
        }
        Ok(())
    }
}

/// Validated selection borrowed from one gateway. No caller can construct a
/// selection that bypasses binding validation. An adapter response is terminal.
#[derive(Clone, Copy, Debug)]
pub struct SelectedRoute<'a> {
    pub(crate) route: &'a Route,
    pub(crate) binding: &'a Binding,
    pub(crate) method_allowed: bool,
}

impl<'a> SelectedRoute<'a> {
    /// Fixed route owner, including its admission policy chain.
    pub fn route(self) -> &'a Route {
        self.route
    }
    /// Validated configured execution resource.
    pub fn binding(self) -> &'a Binding {
        self.binding
    }
    /// False means an owned 405, never a search for another matching route.
    pub fn method_allowed(self) -> bool {
        self.method_allowed
    }
}

pub(crate) fn validate_method(method: &str) -> Result<(), GatewayError> {
    if method.is_empty()
        || method.len() > 32
        || !method
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
    {
        return Err(GatewayError("invalid HTTP method"));
    }
    Ok(())
}

pub(crate) fn normalize_path(path: &str) -> Result<String, GatewayError> {
    if path.len() > MAX_PATH_BYTES || !path.starts_with('/') {
        return Err(GatewayError("gateway path must be a bounded absolute path"));
    }
    let mut decoded = Vec::with_capacity(path.len());
    let mut input = path.bytes();
    while let Some(byte) = input.next() {
        let byte = if byte == b'%' {
            let hi = input.next().and_then(hex);
            let lo = input.next().and_then(hex);
            let (Some(hi), Some(lo)) = (hi, lo) else {
                return Err(GatewayError("invalid percent-encoded path"));
            };
            let value = hi * 16 + lo;
            if matches!(value, b'/' | b'\\' | b'%') {
                return Err(GatewayError("ambiguous encoded path separator"));
            }
            value
        } else {
            byte
        };
        if byte.is_ascii_control() || matches!(byte, b'\\' | b'?' | b'#') {
            return Err(GatewayError("invalid gateway path character"));
        }
        decoded.push(byte);
    }
    let path = String::from_utf8(decoded).map_err(|_| GatewayError("gateway path is not UTF-8"))?;
    if path.contains("//") || path.split('/').any(|s| matches!(s, "." | "..")) {
        return Err(GatewayError("ambiguous gateway path segments"));
    }
    Ok(path)
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
