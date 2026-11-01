//! Session variables from the request context.
//!
//! A [`Session`] is an opaque string map of claims the transport hands the
//! handler. Keys and values are **deployment convention**, not a fixed wire
//! protocol — any trusted gateway (JWT middleware, authenticating ingress,
//! Hasura actions, custom BFF, …) can inject whatever claims your service
//! needs.

use std::collections::HashMap;

/// Key used by [`Session::user_id`].
///
/// Convenience only — not required. Gateways map authenticated subject claims
/// into this key, or handlers read gateway-specific names via [`Session::get`].
pub const USER_ID_KEY: &str = "x-user-id";

/// Key used by [`Session::roles`] — comma-separated asserted engine roles.
///
/// Set-only identity (`x-roles`). There is no singleton primary-role key.
/// Convenience only — not required. See [`USER_ID_KEY`].
pub const ROLE_KEY: &str = "x-roles";

/// Parsed session variables from the incoming request.
///
/// Built from whatever the transport provides: HTTP headers, gRPC metadata,
/// bus message metadata, or a request body's `session_variables` map. The
/// framework does not interpret most keys — handlers read what they need.
///
/// [`Session::user_id`] / [`Session::roles`] are thin helpers over
/// [`USER_ID_KEY`] / [`ROLE_KEY`]. They are not a protocol. Example mappings
/// a trusted edge might perform:
///
/// | Gateway | Example source claim | Inject as (for helpers) |
/// |---|---|---|
/// | JWT middleware | `sub` | `x-user-id` |
/// | OIDC roles claim map | `roles` / `groups` / … | `x-roles` (comma-separated) |
/// | Custom ingress | `X-User-Id` | `x-user-id` (after auth) |
/// | Hasura action | `x-hasura-user-id` | `x-user-id`, or read via `.get("x-hasura-user-id")` |
///
/// # Trust boundary (security-critical)
///
/// **This framework does NOT authenticate.** A `Session` is built from
/// whatever the transport hands it — HTTP request headers, gRPC metadata, or
/// the request payload's `session_variables`. None of that is verified here.
/// Identity values (including those under [`USER_ID_KEY`] / [`ROLE_KEY`]) are
/// trusted at face value by [`Session::user_id`], [`Session::roles`], and any
/// handler that reads them.
///
/// You MUST deploy this behind a **trusted proxy / API gateway** that:
///
/// - **Strips** any client-supplied identity headers/metadata on inbound
///   requests, and
/// - **Injects** only authenticated identity claims it has verified.
///
/// Without that proxy, any caller can set identity keys and assume any
/// identity or role.
///
/// ## Source precedence
///
/// When a request carries identity in more than one place, the **trusted
/// transport channel wins over the client-controlled payload**:
///
/// - **gRPC:** transport metadata (trusted, proxy-injected) overrides payload
///   `session_variables` (client-controlled). See `grpc::build_session`.
/// - **HTTP:** request headers populate the session; the proxy is responsible
///   for ensuring those headers are authenticated. See
///   `http::session_from_headers`.
///
/// Treat metadata/headers as trusted only insofar as your proxy guarantees
/// it; treat the request body as never trustworthy for identity.
#[derive(Debug, Clone, Default)]
pub struct Session {
    variables: HashMap<String, String>,
}

impl Session {
    /// Create an empty session.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a session from a map of variables.
    pub fn from_map(variables: HashMap<String, String>) -> Self {
        Self { variables }
    }

    /// Get the user ID under the convenience key [`USER_ID_KEY`].
    ///
    /// For gateway-specific claim names, use [`Session::get`] instead.
    pub fn user_id(&self) -> Option<&str> {
        self.get(USER_ID_KEY)
    }

    /// Asserted engine roles for this principal ([`ROLE_KEY`] / `x-roles`).
    ///
    /// Set-only identity: multi-role principals list every allowlisted role.
    /// Order is first-seen discovery order from claim mapping.
    pub fn roles(&self) -> Vec<&str> {
        let Some(raw) = self.get(ROLE_KEY).or_else(|| self.get("X-Roles")) else {
            return Vec::new();
        };
        raw.split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect()
    }

    /// True when `role` is a member of the asserted set.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles().contains(&role)
    }

    /// Singleton convenience: the sole asserted role, or `None` when the set
    /// is empty or multi-valued. Prefer [`roles`] / [`has_role`].
    pub fn role(&self) -> Option<&str> {
        let raw = self.get(ROLE_KEY).or_else(|| self.get("X-Roles"))?;
        let mut parts = raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty());
        let first = parts.next()?;
        if parts.next().is_some() {
            None
        } else {
            Some(first)
        }
    }

    /// Get a session variable by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.variables.get(key).map(|v| v.as_str())
    }

    /// Set a session variable.
    pub fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.variables.insert(key.into(), value.into());
    }

    /// Remove a variable, returning its value.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.variables.remove(key)
    }

    /// Whether `key` is set.
    pub fn has(&self, key: &str) -> bool {
        self.variables.contains_key(key)
    }

    /// Get all session variables.
    pub fn variables(&self) -> &HashMap<String, String> {
        &self.variables
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_session() {
        let session = Session::new();
        assert_eq!(session.user_id(), None);
        assert_eq!(session.role(), None);
        assert!(session.roles().is_empty());
        assert!(!session.has("anything"));
    }

    #[test]
    fn convenience_identity_keys() {
        let mut vars = HashMap::new();
        vars.insert(USER_ID_KEY.to_string(), "user-42".to_string());
        vars.insert(ROLE_KEY.to_string(), "customer".to_string());
        let session = Session::from_map(vars);

        assert_eq!(session.user_id(), Some("user-42"));
        assert_eq!(session.roles(), vec!["customer"]);
        assert_eq!(session.role(), Some("customer"));
        assert!(session.has_role("customer"));
        assert!(session.has(USER_ID_KEY));
        assert!(!session.has("x-admin-secret"));
    }

    #[test]
    fn multi_role_set_is_not_a_singleton() {
        let mut session = Session::new();
        session.set(ROLE_KEY, "admin,user");
        assert_eq!(session.roles(), vec!["admin", "user"]);
        assert!(session.has_role("admin"));
        assert!(session.has_role("user"));
        assert_eq!(session.role(), None);
    }

    #[test]
    fn arbitrary_gateway_keys_via_get() {
        // Hasura (or any other gateway) claim names are not special — read
        // them with get() if you keep the gateway's native names.
        let mut session = Session::new();
        session.set("x-hasura-user-id", "hasura-user");
        assert_eq!(session.get("x-hasura-user-id"), Some("hasura-user"));
        assert_eq!(session.user_id(), None); // convenience key not set
    }

    #[test]
    fn set_and_get() {
        let mut session = Session::new();
        session.set(ROLE_KEY, "admin");
        assert_eq!(session.get(ROLE_KEY), Some("admin"));
        assert_eq!(session.roles(), vec!["admin"]);
        assert_eq!(session.role(), Some("admin"));
    }
}
