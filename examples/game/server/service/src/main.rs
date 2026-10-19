//! `mobpit-service` — Mob Pit's player-stats read model behind GraphQL `@live`.
//!
//! Env (dev defaults in `examples/game/run.sh`):
//! - `DATABASE_URL` — read model, `sqlite:…` (default `sqlite:./mobpit-service.db`)
//! - `MOBPIT_BUS_URL` — the SQLite bus the zone server publishes Crawler
//!   domain events to (default `sqlite:./mobpit-bus.db`)
//! - `BIND` (default `127.0.0.1:8791`, the `distributed dev` convention)
//! - `MOBPIT_API_ISSUER`, `MOBPIT_API_AUDIENCE` — expected `iss` / `aud` of the
//!   Bearer JWT (required)
//! - `MOBPIT_API_JWKS` — static JWKS JSON (dev: the SvelteKit server's ES256
//!   public key); unset = discover from the issuer (`/.well-known/openid-configuration`)
//! - `MOBPIT_PROTOCOL_KEY` — 64 hex chars for the generated clients'
//!   protocol tokens (required)
//! - `MOBPIT_UI_ORIGINS` — comma-separated browser origins allowed to call
//!   `/graphql` (the SvelteKit app)

use std::env;

use mobpit_service::{run, BearerIdentity, ServiceOptions};

fn required(name: &str) -> Result<String, String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is required"))
}

fn protocol_key() -> Result<[u8; 32], String> {
    let hex = required("MOBPIT_PROTOCOL_KEY")?;
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2).unwrap_or("x"), 16))
        .collect::<Result<_, _>>()
        .map_err(|_| "MOBPIT_PROTOCOL_KEY must be hex".to_string())?;
    let key: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "MOBPIT_PROTOCOL_KEY must be 32 bytes".to_string())?;
    if key.iter().all(|b| *b == 0) {
        return Err("MOBPIT_PROTOCOL_KEY must not be all zero".into());
    }
    Ok(key)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let options = ServiceOptions {
        database_url: env::var("DATABASE_URL")
            .unwrap_or_else(|_| "sqlite:./mobpit-service.db?mode=rwc".into()),
        bus_url: env::var("MOBPIT_BUS_URL").unwrap_or_else(|_| "sqlite:./mobpit-bus.db".into()),
        bind: env::var("BIND").unwrap_or_else(|_| "127.0.0.1:8791".into()),
        identity: BearerIdentity {
            issuer: required("MOBPIT_API_ISSUER")?,
            audience: required("MOBPIT_API_AUDIENCE")?,
            jwks: env::var("MOBPIT_API_JWKS")
                .ok()
                .filter(|v| !v.trim().is_empty()),
        },
        protocol_key: protocol_key()?,
        allowed_origins: env::var("MOBPIT_UI_ORIGINS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|origin| !origin.is_empty())
            .map(String::from)
            .collect(),
    };
    run(options).await
}
