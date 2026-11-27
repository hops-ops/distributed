//! e2e-ui runner — one-screen host invocation.
//!
//! Env:
//! - `DATABASE_URL` — `sqlite:…` (default) or `postgres://…`
//! - `BIND` (default `127.0.0.1:8791`)
//! - `OIDC_ISSUER` / `OIDC_AUDIENCE` / `OIDC_JWKS_URI` → OidcBearer; else DevHeaders
//! - `ZITADEL_SERVICE_USER_TOKEN` + `OIDC_ISSUER`/`ZITADEL_API_URL` → periodic user scrape
//! - `ZITADEL_SCRAPE_INTERVAL_SECS` (default 60; `0` = no background loop)

use std::env;

use e2e_service::{identity_from_env, run, HostOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite:./e2e-ui.db?mode=rwc".into());
    let bind = env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8791".into());
    run(
        &database_url,
        HostOptions {
            bind,
            identity: identity_from_env(),
            public_origin: env::var("PUBLIC_ORIGIN")
                .unwrap_or_else(|_| "http://localhost:8791".into()),
            ui_origin: env::var("UI_INTERNAL_ORIGIN")
                .unwrap_or_else(|_| "http://localhost:5180".into()),
            delivery: match env::var("GATEWAY_DELIVERY").as_deref().unwrap_or("none") {
                "none" => Default::default(),
                "all" => distributed::gateway::DeliveryCapabilities {
                    snapshots: true,
                    coalescing: true,
                    live_sharing: true,
                },
                _ => return Err("GATEWAY_DELIVERY must be none or all".into()),
            },
        },
    )
    .await
}
