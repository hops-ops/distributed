//! Role vocabulary for GraphQL permissions.

pub const USER: &str = "user";
pub const ANONYMOUS: &str = "anonymous";

/// Roles declared on the engine builder.
pub const ALL: &[&str] = &[USER, ANONYMOUS];
