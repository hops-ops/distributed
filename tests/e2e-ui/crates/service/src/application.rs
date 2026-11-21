//! e2e-ui application composition root.
//!
//! This is the review-visible product declaration: surface identities, module
//! inventory, and re-exports of the composed host APIs. Infrastructure
//! (dialect, outbox, OIDC serve) stays in `host`; handlers stay in modules.

use crate::modules::{blob, chat, compose, identity, todo};

/// Stable normal-application surface shared by user and admin sessions.
pub const DISTRIBUTED_CLIENT_SURFACE: &str = "e2e-ui";
/// Stable elevated surface for routes that intentionally include admin-only fields.
pub const DISTRIBUTED_ADMIN_CLIENT_SURFACE: &str = "e2e-ui-admin";
/// Unauthenticated public surface (lobby message peek).
pub const DISTRIBUTED_PUBLIC_CLIENT_SURFACE: &str = "e2e-ui-public";

/// Logical application name used for manifest / plan identity.
pub const E2E_UI_APPLICATION: &str = "e2e-ui";

/// Explicit module identities owned by the e2e application.
pub const E2E_UI_MODULE_IDS: &[&str] = compose::MODULE_IDS;

/// Compile-time proof that module inventory matches bounded-context crates.
pub const MODULE_DECLARATIONS: &[(&str, &str)] = &[
    (todo::MODULE_ID, "todo commands + projector"),
    (chat::MODULE_ID, "chat commands + projector"),
    (blob::MODULE_ID, "blob Atomic commands"),
    (
        identity::MODULE_ID,
        "Zitadel ingress/scrape commands + AuthUsers projector",
    ),
];
