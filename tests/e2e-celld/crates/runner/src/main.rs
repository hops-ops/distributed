//! Celld example runner (not `tests/e2e-ui` / `make run`).
//!
//! Env:
//! - `CELLD_URL` — required
//! - `NATS_URL` — required (JetStream; Eventual projectors subscribe here)
//! - `DATABASE_URL` — required `postgres://…` (from `make -C tests/e2e-ui up`)
//! - `BIND` (default `127.0.0.1:8791`)
//! - `OIDC_*` → OidcBearer; else DevHeaders

use std::env;

use distributed::cell_host::{InternalHttpSecret, CELL_INTERNAL_SECRET_ENV};
use e2e_celld_graphql::{identity_from_env, run, HostOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_url = env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is required (postgres://…). Start: make -C tests/e2e-ui up")?;
    if !(database_url.starts_with("postgres://") || database_url.starts_with("postgresql://")) {
        return Err("e2e-celld requires Postgres DATABASE_URL (not sqlite)".into());
    }
    let bind = env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8791".into());
    let celld_url = env::var("CELLD_URL")
        .map_err(|_| "CELLD_URL is required. Start infra: make -C tests/e2e-ui up-celld-nats")?;
    let nats_url = env::var("NATS_URL")
        .map_err(|_| "NATS_URL is required. Start infra: make -C tests/e2e-ui up-celld-nats")?;
    let internal_secret = InternalHttpSecret::new(
        env::var(CELL_INTERNAL_SECRET_ENV)
            .map_err(|_| "DISTRIBUTED_INTERNAL_SECRET is required")?,
    )?;
    eprintln!("e2e-celld CELLD_URL={celld_url} NATS_URL={nats_url}");
    run(
        &database_url,
        HostOptions {
            bind,
            public_origin: env::var("PUBLIC_ORIGIN")
                .unwrap_or_else(|_| "http://localhost:8791".into()),
            ui_origin: env::var("UI_INTERNAL_ORIGIN")
                .unwrap_or_else(|_| "http://localhost:5180".into()),
            identity: identity_from_env(),
            celld_url,
            nats_url,
            internal_secret,
        },
    )
    .await
}
