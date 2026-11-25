//! e2e-ui application composition root.
//!
//! This is the review-visible product declaration: the application name and
//! its generated client surfaces. Modules are the Service's command
//! namespaces (`modules::compose`); infrastructure stays in `host`.

/// Logical application name used for manifest / plan identity.
pub const E2E_UI_APPLICATION: &str = "e2e-ui";

/// One generated client surface: who may open it, and whose grants shape its
/// schema. Declared once; the GraphQL engine registers it and the client
/// export selects it.
#[derive(Clone, Copy, Debug)]
pub struct ClientSurface {
    pub name: &'static str,
    pub eligible_roles: &'static [&'static str],
    pub schema_roles: &'static [&'static str],
}

/// Normal-application surface shared by user and admin sessions.
pub const CLIENT_SURFACE: ClientSurface = ClientSurface {
    name: "e2e-ui",
    eligible_roles: &["admin", "user"],
    schema_roles: &["user"],
};
/// Elevated surface for routes that intentionally include admin-only fields.
pub const ADMIN_CLIENT_SURFACE: ClientSurface = ClientSurface {
    name: "e2e-ui-admin",
    eligible_roles: &["admin"],
    schema_roles: &["admin"],
};
/// Unauthenticated public surface (lobby message peek).
pub const PUBLIC_CLIENT_SURFACE: ClientSurface = ClientSurface {
    name: "e2e-ui-public",
    eligible_roles: &["anonymous"],
    schema_roles: &["anonymous"],
};

/// Every client surface the application serves.
pub const CLIENT_SURFACES: [ClientSurface; 3] =
    [CLIENT_SURFACE, ADMIN_CLIENT_SURFACE, PUBLIC_CLIENT_SURFACE];
