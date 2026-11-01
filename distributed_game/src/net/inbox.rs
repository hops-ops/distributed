//! Zone inbox over the network: [`GameServer::send`](super::GameServer::send)
//! locally, `POST /admin/zone-message` for peers and services, and
//! [`ZoneInboxClient`] for services (see [`crate::inbox`]).
//!
//! `POST /admin/zone-message` (header `x-game-admin-secret`), JSON body:
//!
//! ```text
//! {"zone": "pit-1", "kick": {"principal": "u-42", "reason": "banned"}}
//! {"zone": "pit-1", "message": {"type_id": 3, "payload": "<base64url>"}}
//! ```
//!
//! | Status | Body | Meaning |
//! |---|---|---|
//! | 200 | `{ok, players_kicked, observers_kicked}` | delivered on the zone task |
//! | 400 | `{error}` | malformed body, unknown message id or bad payload |
//! | 401 | `{error: "unauthorized"}` | missing / wrong admin secret (or none configured) |
//! | 404 | `{error: "inactive"}` | the zone is not running here (never activated by a message) |
//! | 421 | `{error: "wrong_owner", owner, admin_url}` | another host owns the zone (directory) |

use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde_json::{json, Value};

use super::peer::post_json;
use crate::authoring::GameEvent;
use crate::inbox::{Delivered, ZoneInbound};
use crate::world::{HostAddr, ZoneDirectory};

/// Route path.
pub const ZONE_MESSAGE_PATH: &str = "/admin/zone-message";

/// Why a message was not delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxError {
    /// The zone is not active on the host asked (it is not activated).
    Inactive,
    /// Another host owns the zone.
    WrongOwner(HostAddr),
    /// Malformed message (unknown id, payload that does not decode).
    Invalid(String),
    /// The admin secret was refused.
    Unauthorized,
    /// Transport failure or an unexpected answer.
    Unavailable(String),
}

impl std::fmt::Display for InboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inactive => f.write_str("zone not active"),
            Self::WrongOwner(host) => write!(f, "zone owned by {}", host.id),
            Self::Invalid(why) => write!(f, "invalid zone message: {why}"),
            Self::Unauthorized => f.write_str("admin secret refused"),
            Self::Unavailable(why) => write!(f, "zone inbox unavailable: {why}"),
        }
    }
}

impl std::error::Error for InboxError {}

/// A zone message on the wire between tasks and hosts: decoded into
/// `Z::Message` on the zone's own task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WireInbound {
    Kick { principal: String, reason: String },
    Message { type_id: u32, payload: Vec<u8> },
}

impl WireInbound {
    pub(crate) fn of<M: GameEvent>(inbound: &ZoneInbound<M>) -> Self {
        match inbound {
            ZoneInbound::Kick { principal, reason } => Self::Kick {
                principal: principal.clone(),
                reason: reason.clone(),
            },
            ZoneInbound::Message(message) => Self::Message {
                type_id: message.event_id(),
                payload: message.to_payload(),
            },
        }
    }

    pub(crate) fn decode<M: GameEvent>(self) -> Result<ZoneInbound<M>, InboxError> {
        match self {
            Self::Kick { principal, reason } => {
                if principal.is_empty() {
                    return Err(InboxError::Invalid("kick needs a principal".into()));
                }
                Ok(ZoneInbound::Kick { principal, reason })
            }
            Self::Message { type_id, payload } => match M::decode_event(type_id, &payload) {
                Ok(Some(message)) => Ok(ZoneInbound::Message(message)),
                Ok(None) => Err(InboxError::Invalid(format!(
                    "this zone declares no message {type_id}"
                ))),
                Err(e) => Err(InboxError::Invalid(format!("message {type_id}: {e:?}"))),
            },
        }
    }

    pub(crate) fn to_json(&self, zone: &str) -> Value {
        match self {
            Self::Kick { principal, reason } => {
                json!({ "zone": zone, "kick": { "principal": principal, "reason": reason } })
            }
            Self::Message { type_id, payload } => json!({
                "zone": zone,
                "message": { "type_id": type_id, "payload": URL_SAFE_NO_PAD.encode(payload) },
            }),
        }
    }

    /// `(zone, inbound)` from a route body.
    pub(crate) fn from_json(body: &Value) -> Result<(String, Self), String> {
        let zone = body
            .get("zone")
            .and_then(Value::as_str)
            .filter(|z| !z.is_empty())
            .ok_or("zone")?
            .to_string();
        let text = |v: &Value, name: &str| v.get(name).and_then(Value::as_str).map(str::to_string);
        let inbound = match (body.get("kick"), body.get("message")) {
            (Some(kick), None) => Self::Kick {
                principal: text(kick, "principal").ok_or("kick.principal")?,
                reason: text(kick, "reason").unwrap_or_default(),
            },
            (None, Some(message)) => Self::Message {
                type_id: message
                    .get("type_id")
                    .and_then(Value::as_u64)
                    .and_then(|id| u32::try_from(id).ok())
                    .ok_or("message.type_id")?,
                payload: URL_SAFE_NO_PAD
                    .decode(text(message, "payload").ok_or("message.payload")?)
                    .map_err(|_| "message.payload")?,
            },
            _ => return Err("exactly one of `kick` or `message`".into()),
        };
        Ok((zone, inbound))
    }
}

pub(crate) fn delivered_json(delivered: Delivered) -> Value {
    json!({
        "ok": true,
        "players_kicked": delivered.players_kicked,
        "observers_kicked": delivered.observers_kicked,
    })
}

/// Sends zone messages from a service: finds the owning host through the
/// [`ZoneDirectory`], posts to its `/admin/zone-message` and follows
/// `wrong_owner` answers (ownership moved) a bounded number of times.
#[derive(Clone)]
pub struct ZoneInboxClient {
    directory: Arc<dyn ZoneDirectory>,
    secret: String,
    timeout: Duration,
    max_redirects: u32,
}

impl std::fmt::Debug for ZoneInboxClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZoneInboxClient")
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

impl ZoneInboxClient {
    /// Client over `directory` with the hosts' shared admin secret.
    pub fn new(directory: Arc<dyn ZoneDirectory>, admin_secret: impl Into<String>) -> Self {
        Self {
            directory,
            secret: admin_secret.into(),
            timeout: Duration::from_secs(3),
            max_redirects: 3,
        }
    }

    /// Per-request timeout (default 3 s).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Deliver a typed game message to `zone`.
    pub async fn send<M: GameEvent>(&self, zone: &str, message: &M) -> Result<(), InboxError> {
        self.post(
            zone,
            WireInbound::Message {
                type_id: message.event_id(),
                payload: message.to_payload(),
            },
        )
        .await
        .map(drop)
    }

    /// Remove every connection of `principal` from `zone`.
    pub async fn kick(
        &self,
        zone: &str,
        principal: &str,
        reason: &str,
    ) -> Result<Delivered, InboxError> {
        self.post(
            zone,
            WireInbound::Kick {
                principal: principal.into(),
                reason: reason.into(),
            },
        )
        .await
    }

    async fn post(&self, zone: &str, inbound: WireInbound) -> Result<Delivered, InboxError> {
        let body = inbound.to_json(zone);
        let mut admin_url = self
            .directory
            .locate(zone)
            .ok_or(InboxError::Inactive)?
            .admin_url;
        for _ in 0..=self.max_redirects {
            let (status, reply) = post_json(
                &admin_url,
                ZONE_MESSAGE_PATH,
                &self.secret,
                &body,
                self.timeout,
            )
            .await
            .map_err(InboxError::Unavailable)?;
            let error = || {
                reply
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string()
            };
            match status {
                200 => {
                    let count =
                        |name: &str| reply.get(name).and_then(Value::as_u64).unwrap_or(0) as usize;
                    return Ok(Delivered {
                        players_kicked: count("players_kicked"),
                        observers_kicked: count("observers_kicked"),
                    });
                }
                421 => {
                    admin_url = reply
                        .get("admin_url")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            InboxError::Unavailable("wrong_owner without admin_url".into())
                        })?
                        .to_string();
                }
                400 => return Err(InboxError::Invalid(error())),
                401 => return Err(InboxError::Unauthorized),
                404 => return Err(InboxError::Inactive),
                other => return Err(InboxError::Unavailable(format!("{other}: {}", error()))),
            }
        }
        Err(InboxError::Unavailable(
            "too many wrong_owner redirects".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_bodies_round_trip() {
        for inbound in [
            WireInbound::Kick {
                principal: "u-1".into(),
                reason: "banned".into(),
            },
            WireInbound::Message {
                type_id: 7,
                payload: vec![1, 2, 255],
            },
        ] {
            let (zone, back) = WireInbound::from_json(&inbound.to_json("pit-1")).unwrap();
            assert_eq!((zone.as_str(), back), ("pit-1", inbound));
        }
        assert!(WireInbound::from_json(&json!({"zone": "z", "kick": {}})).is_err());
        assert!(WireInbound::from_json(&json!({"zone": "z"})).is_err());
        assert!(WireInbound::from_json(
            &json!({"zone": "z", "kick": {"principal": "p"}, "message": {"type_id": 1, "payload": ""}})
        )
        .is_err());
    }
}
