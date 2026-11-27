//! The dev ticket route's wire types (`POST /dev/ticket`, mounted by Mob
//! Pit's services with `--dev`): `{zone, name?, role?, guest?}` →
//! `{ticket, principal, name, guest, zone, expiresAt}`, a single-use HS256
//! admission ticket for a **server-generated** guest principal. The caller
//! chooses only a display name; `guest` (a token from an earlier response,
//! HMAC-signed by the server) keeps the same principal across page
//! reloads. A real game gets tickets from its authenticated lobby.

use serde::{Deserialize, Serialize};

/// Ticket lifetime: long enough to connect, short because it is single use.
pub const TICKET_TTL_SECS: u64 = 120;

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
