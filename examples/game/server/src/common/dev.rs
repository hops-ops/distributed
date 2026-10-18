//! **Dev-only** HTTP routes, mounted only with `--dev` / `DG_DEMO_DEV=1`.
//!
//! - `POST /dev/ticket` `{zone, name?, role?, guest?}` →
//!   `{ticket, principal, name, guest, zone, expiresAt}`. Mints a single-use
//!   HS256 admission ticket for a **server-generated** guest principal. The
//!   caller chooses only a display name; `guest` (a token from an earlier
//!   response, HMAC-signed by this server) keeps the same principal across
//!   page reloads. A real game gets tickets from its authenticated lobby.
//! - `GET /dev/netsim` / `POST /dev/netsim` `{latencyMs, jitterMs, loss}` —
//!   read / replace the server-side network simulation for **every**
//!   connection (one-way latency applied in both directions, so RTT grows by
//!   2 × latency; loss is a TCP-retransmit delay, see `NetSim`).

use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use distributed_game::net::{GameServer, NetSim, TicketKey};
use distributed_game::{Role, TicketClaims};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::guests::{self, Guests};
use super::Demo;

/// Ticket lifetime: long enough to connect, short because it is single use.
pub const TICKET_TTL_SECS: u64 = 120;

struct DevState<D: Demo> {
    server: GameServer<D::Zone>,
    key: TicketKey,
    guests: Guests,
}

impl<D: Demo> Clone for DevState<D> {
    fn clone(&self) -> Self {
        Self {
            server: self.server.clone(),
            key: self.key.clone(),
            guests: self.guests.clone(),
        }
    }
}

/// `POST /dev/ticket` body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TicketRequest {
    /// Zone to join (must be hosted by the demo).
    pub zone: String,
    /// Display name (sanitized; identity is never taken from it).
    #[serde(default)]
    pub name: Option<String>,
    /// `"player"` (default) or `"observer"`.
    #[serde(default)]
    pub role: Option<String>,
    /// Guest token from an earlier response.
    #[serde(default)]
    pub guest: Option<String>,
}

/// `POST /dev/ticket` response.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TicketResponse {
    /// HS256 admission ticket for Hello.
    pub ticket: String,
    /// Server-generated principal.
    pub principal: String,
    /// Display name in effect.
    pub name: String,
    /// Present this as `guest` next time to keep the principal.
    pub guest: String,
    /// Zone the ticket admits to.
    pub zone: String,
    /// Unix seconds.
    pub expires_at: u64,
}

/// NetSim as JSON (milliseconds, loss as a fraction).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct NetSimJson {
    /// One-way latency.
    pub latency_ms: u64,
    /// Uniform extra delay in `[0, jitter]`.
    pub jitter_ms: u64,
    /// Fraction of messages delayed by a simulated retransmit.
    pub loss: f32,
}

impl NetSimJson {
    fn from_sim(sim: Option<NetSim>) -> Self {
        sim.map_or_else(Self::default, |s| Self {
            latency_ms: s.latency.as_millis() as u64,
            jitter_ms: s.jitter.as_millis() as u64,
            loss: s.loss,
        })
    }

    fn to_sim(self) -> Option<NetSim> {
        let latency = Duration::from_millis(self.latency_ms.min(1000));
        let jitter = Duration::from_millis(self.jitter_ms.min(500));
        let loss = if self.loss.is_finite() {
            self.loss.clamp(0.0, 0.5)
        } else {
            0.0
        };
        (!latency.is_zero() || !jitter.is_zero() || loss > 0.0)
            .then(|| NetSim::new(latency, jitter).with_loss(loss, Duration::from_millis(200)))
    }
}

/// The dev router for demo `D`.
pub fn routes<D: Demo>(server: GameServer<D::Zone>, key: TicketKey, guests: Guests) -> Router {
    Router::new()
        .route("/dev/ticket", post(ticket::<D>))
        .route("/dev/netsim", get(get_netsim::<D>).post(set_netsim::<D>))
        .with_state(DevState::<D> {
            server,
            key,
            guests,
        })
}

type ApiError = (StatusCode, Json<serde_json::Value>);

fn bad_request(message: &str) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": message })),
    )
}

async fn ticket<D: Demo>(
    State(state): State<DevState<D>>,
    Json(request): Json<TicketRequest>,
) -> Result<Json<TicketResponse>, ApiError> {
    if !D::hosts_zone(&request.zone) {
        return Err(bad_request("zone is not hosted by this demo"));
    }
    let role = match request.role.as_deref() {
        None | Some("player") => Role::Player,
        Some("observer") => Role::Observer,
        Some(_) => return Err(bad_request("role must be player or observer")),
    };
    // Identity: a verified guest token, else a brand-new principal.
    let principal = request
        .guest
        .as_deref()
        .and_then(|token| guests::verify_guest_token(&state.key, token))
        .unwrap_or_else(guests::new_principal);
    let name = request
        .name
        .as_deref()
        .map(guests::sanitize_name)
        .filter(|n| !n.is_empty())
        .or_else(|| state.guests.name(&principal))
        .unwrap_or_else(|| format!("Guest {}", principal.get(6..10).unwrap_or("")));
    state.guests.set_name(&principal, name.clone());
    let expires_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
        + TICKET_TTL_SECS;
    let ticket = state.key.sign(&TicketClaims {
        principal: principal.clone(),
        zone: request.zone.clone(),
        expires_at,
        nonce: guests::random_hex(24),
        role,
    });
    Ok(Json(TicketResponse {
        ticket,
        guest: guests::guest_token(&state.key, &principal),
        principal,
        name,
        zone: request.zone,
        expires_at,
    }))
}

async fn get_netsim<D: Demo>(State(state): State<DevState<D>>) -> Json<NetSimJson> {
    Json(NetSimJson::from_sim(state.server.net_sim()))
}

async fn set_netsim<D: Demo>(
    State(state): State<DevState<D>>,
    Json(body): Json<NetSimJson>,
) -> Json<NetSimJson> {
    state.server.set_net_sim(body.to_sim());
    Json(NetSimJson::from_sim(state.server.net_sim()))
}
