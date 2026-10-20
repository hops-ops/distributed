//! Message types and their codecs (`PROTOCOL.md` §6–§7).

use std::collections::BTreeMap;

use super::wire::{DecodeError, Reader, Writer};
use crate::schema::{EntitySchema, FieldValue};

/// Wire protocol version spoken by this crate.
pub const PROTOCOL_VERSION: u32 = 1;

/// Message type bytes.
pub mod tag {
    /// Client → server Hello.
    pub const HELLO: u8 = 0x01;
    /// Client → server Input.
    pub const INPUT: u8 = 0x02;
    /// Client → server Ping.
    pub const PING: u8 = 0x03;
    /// Client → server Leave.
    pub const LEAVE: u8 = 0x04;
    /// Server → client Welcome.
    pub const WELCOME: u8 = 0x80;
    /// Server → client Delta.
    pub const DELTA: u8 = 0x81;
    /// Server → client Event.
    pub const EVENT: u8 = 0x82;
    /// Server → client Pong.
    pub const PONG: u8 = 0x83;
    /// Server → client Freeze.
    pub const FREEZE: u8 = 0x84;
    /// Server → client Resume.
    pub const RESUME: u8 = 0x85;
    /// Server → client Handoff.
    pub const HANDOFF: u8 = 0x86;
    /// Server → client Kick.
    pub const KICK: u8 = 0x87;
    /// Server → client Error.
    pub const ERROR: u8 = 0x88;
    /// Server → client Redirect (zone owned by another host).
    pub const REDIRECT: u8 = 0x89;
}

/// Kick reason codes.
pub mod kick {
    /// Removed by zone logic.
    pub const BY_ZONE: u32 = 1;
    /// Same principal connected again; this connection was replaced.
    pub const REPLACED: u32 = 2;
    /// Server shutting down.
    pub const SHUTDOWN: u32 = 3;
    /// Input rate limit abuse.
    pub const RATE_LIMITED: u32 = 4;
    /// Zone disposed.
    pub const ZONE_DISPOSED: u32 = 5;
    /// Outbound queue overflow (client too slow).
    pub const SLOW_CONSUMER: u32 = 6;
    /// Client content is stale: reload, then reconnect with the last
    /// Welcome's reconnect token inside the grace window.
    pub const RELOAD: u32 = 7;
}

/// Error codes.
pub mod error {
    /// Malformed or unexpected message.
    pub const BAD_MESSAGE: u32 = 1;
    /// Ticket missing, invalid, expired or replayed.
    pub const UNAUTHORIZED: u32 = 2;
    /// Zone `on_auth` rejected the join.
    pub const REJECTED: u32 = 3;
    /// Zone at capacity.
    pub const ZONE_FULL: u32 = 4;
    /// Protocol or content version mismatch.
    pub const VERSION_MISMATCH: u32 = 5;
    /// Input dropped by rate limiting (sent at most once per second).
    pub const RATE_LIMITED: u32 = 6;
    /// Unknown zone.
    pub const NOT_FOUND: u32 = 7;
    /// Reconnect token unknown or grace window elapsed.
    pub const RECONNECT_FAILED: u32 = 8;
}

/// Client handshake.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Hello {
    /// Must equal [`PROTOCOL_VERSION`].
    pub protocol_version: u32,
    /// Game content version the client was built against.
    pub content_version: String,
    /// Game-defined capability bits (opaque to the runtime).
    pub capabilities: u32,
    /// Signed admission ticket (empty when reconnecting).
    pub ticket: String,
    /// Reconnect token from a previous Welcome (empty for a fresh join).
    pub reconnect_token: String,
}

/// Messages a client sends.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientMessage {
    /// Handshake; must be the first message.
    Hello(Hello),
    /// Sequenced input.
    Input {
        /// Strictly increasing per connection lifetime, starting at 1.
        seq: u32,
        /// Client's predicted tick when sampled.
        client_tick: u32,
        /// Game-defined input bytes.
        payload: Vec<u8>,
    },
    /// Clock sync probe.
    Ping {
        /// Client clock in ms (echoed).
        client_time: f64,
    },
    /// Graceful leave (no reconnect grace).
    Leave,
}

impl ClientMessage {
    /// Encode one message (one WebSocket binary frame).
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        match self {
            Self::Hello(h) => {
                w.u8(tag::HELLO)
                    .varuint(h.protocol_version)
                    .string(&h.content_version)
                    .varuint(h.capabilities)
                    .string(&h.ticket)
                    .string(&h.reconnect_token);
            }
            Self::Input {
                seq,
                client_tick,
                payload,
            } => {
                w.u8(tag::INPUT)
                    .varuint(*seq)
                    .varuint(*client_tick)
                    .bytes(payload);
            }
            Self::Ping { client_time } => {
                w.u8(tag::PING).f64(*client_time);
            }
            Self::Leave => {
                w.u8(tag::LEAVE);
            }
        }
        w.into_bytes()
    }

    /// Decode one message.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader::new(bytes);
        let message = match r.u8()? {
            tag::HELLO => Self::Hello(Hello {
                protocol_version: r.varuint()?,
                content_version: r.string()?,
                capabilities: r.varuint()?,
                ticket: r.string()?,
                reconnect_token: r.string()?,
            }),
            tag::INPUT => Self::Input {
                seq: r.varuint()?,
                client_tick: r.varuint()?,
                payload: r.bytes()?.to_vec(),
            },
            tag::PING => Self::Ping {
                client_time: r.f64()?,
            },
            tag::LEAVE => Self::Leave,
            other => return Err(DecodeError::UnknownMessage(other)),
        };
        r.finish()?;
        Ok(message)
    }
}

/// Connection role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Owns a player in the zone. Wire `0`.
    Player,
    /// Read-only observer. Wire `1`.
    Observer,
}

/// Server handshake reply.
#[derive(Debug, Clone, PartialEq)]
pub struct Welcome {
    /// Opaque session id (stable across reconnects of the same player).
    pub session_id: String,
    /// Zone joined.
    pub zone_id: String,
    /// Admitted role.
    pub role: Role,
    /// Simulation ticks per second (Hot tier).
    pub tick_rate: u32,
    /// Deltas per second.
    pub patch_rate: u32,
    /// Current server tick.
    pub server_tick: u32,
    /// Server clock in ms.
    pub server_time: f64,
    /// Token for Hello.reconnect_token after a drop.
    pub reconnect_token: String,
    /// Server content version.
    pub content_version: String,
    /// Replicated entity schema.
    pub schema: EntitySchema,
}

/// Full entity state for a create.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityCreate {
    /// Entity id.
    pub id: u32,
    /// Kind id.
    pub kind: u16,
    /// Every field, schema order.
    pub fields: Vec<FieldValue>,
}

/// Changed fields of a known entity.
#[derive(Debug, Clone, PartialEq)]
pub struct EntityUpdate {
    /// Entity id.
    pub id: u32,
    /// Bit `i` set = field `i` present.
    pub mask: u32,
    /// Present fields in ascending index order.
    pub fields: Vec<FieldValue>,
}

/// One replication patch.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Delta {
    /// Server tick the state belongs to.
    pub server_tick: u32,
    /// Highest processed input sequence of this client (0 = none).
    pub ack_seq: u32,
    /// Ids to forget (applied first).
    pub removes: Vec<u32>,
    /// New entities (applied second).
    pub creates: Vec<EntityCreate>,
    /// Field changes (applied last).
    pub updates: Vec<EntityUpdate>,
}

/// Freeze/Resume notice.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Notice {
    /// Machine-readable reason.
    pub reason: String,
    /// Version involved (e.g. content version being rolled out).
    pub version: String,
    /// Human-readable notes.
    pub notes: String,
}

/// Messages the server sends.
#[derive(Debug, Clone, PartialEq)]
pub enum ServerMessage {
    /// Handshake accepted.
    Welcome(Welcome),
    /// Replication patch.
    Delta(Delta),
    /// Reliable game event.
    Event {
        /// Game-defined event type.
        type_id: u32,
        /// Game-defined payload.
        payload: Vec<u8>,
    },
    /// Clock sync reply.
    Pong {
        /// Echoed client time (ms).
        client_time: f64,
        /// Server clock (ms).
        server_time: f64,
    },
    /// Simulation frozen (inputs ignored until Resume).
    Freeze(Notice),
    /// Simulation resumed.
    Resume(Notice),
    /// Move to another zone.
    Handoff {
        /// Destination zone id.
        target_zone: String,
        /// Ticket to present in the next Hello.
        transfer_ticket: String,
    },
    /// Removed from the zone; the server closes the socket.
    Kick {
        /// See [`kick`].
        code: u32,
        /// Text.
        message: String,
    },
    /// Request-level error; handshake errors are followed by close.
    Error {
        /// See [`error`].
        code: u32,
        /// Text.
        message: String,
    },
    /// This host does not own `zone_id`: reconnect to `url` and send the
    /// same Hello again (the ticket was not consumed). Followed by close.
    Redirect {
        /// Zone the client asked for.
        zone_id: String,
        /// Full WebSocket URL of the owning host's zone endpoint.
        url: String,
    },
}

/// What a Delta decoder/encoder needs to know about the client's view.
pub trait EntityKinds {
    /// Schema from the Welcome.
    fn schema(&self) -> Option<&EntitySchema>;
    /// Kind of a known entity id.
    fn kind_of(&self, id: u32) -> Option<u16>;
}

/// No schema: Deltas cannot be decoded.
impl EntityKinds for () {
    fn schema(&self) -> Option<&EntitySchema> {
        None
    }
    fn kind_of(&self, _: u32) -> Option<u16> {
        None
    }
}

/// Schema plus an explicit id→kind table (fixtures/tests).
impl EntityKinds for (EntitySchema, BTreeMap<u32, u16>) {
    fn schema(&self) -> Option<&EntitySchema> {
        Some(&self.0)
    }
    fn kind_of(&self, id: u32) -> Option<u16> {
        self.1.get(&id).copied()
    }
}

impl ServerMessage {
    /// Encode one message. Deltas need `kinds` (schema + known ids).
    pub fn encode(&self, kinds: &dyn EntityKinds) -> Result<Vec<u8>, DecodeError> {
        let mut w = Writer::new();
        match self {
            Self::Welcome(m) => {
                w.u8(tag::WELCOME)
                    .string(&m.session_id)
                    .string(&m.zone_id)
                    .u8(match m.role {
                        Role::Player => 0,
                        Role::Observer => 1,
                    })
                    .varuint(m.tick_rate)
                    .varuint(m.patch_rate)
                    .varuint(m.server_tick)
                    .f64(m.server_time)
                    .string(&m.reconnect_token)
                    .string(&m.content_version);
                m.schema.encode(&mut w);
            }
            Self::Delta(d) => encode_delta(&mut w, d, kinds)?,
            Self::Event { type_id, payload } => {
                w.u8(tag::EVENT).varuint(*type_id).bytes(payload);
            }
            Self::Pong {
                client_time,
                server_time,
            } => {
                w.u8(tag::PONG).f64(*client_time).f64(*server_time);
            }
            Self::Freeze(n) | Self::Resume(n) => {
                let t = if matches!(self, Self::Freeze(_)) {
                    tag::FREEZE
                } else {
                    tag::RESUME
                };
                w.u8(t)
                    .string(&n.reason)
                    .string(&n.version)
                    .string(&n.notes);
            }
            Self::Handoff {
                target_zone,
                transfer_ticket,
            } => {
                w.u8(tag::HANDOFF)
                    .string(target_zone)
                    .string(transfer_ticket);
            }
            Self::Kick { code, message } => {
                w.u8(tag::KICK).varuint(*code).string(message);
            }
            Self::Error { code, message } => {
                w.u8(tag::ERROR).varuint(*code).string(message);
            }
            Self::Redirect { zone_id, url } => {
                w.u8(tag::REDIRECT).string(zone_id).string(url);
            }
        }
        Ok(w.into_bytes())
    }

    /// Decode one message. Deltas need `kinds`.
    pub fn decode(bytes: &[u8], kinds: &dyn EntityKinds) -> Result<Self, DecodeError> {
        let mut r = Reader::new(bytes);
        let message = match r.u8()? {
            tag::WELCOME => Self::Welcome(Welcome {
                session_id: r.string()?,
                zone_id: r.string()?,
                role: match r.u8()? {
                    0 => Role::Player,
                    1 => Role::Observer,
                    _ => return Err(DecodeError::Invalid("role")),
                },
                tick_rate: r.varuint()?,
                patch_rate: r.varuint()?,
                server_tick: r.varuint()?,
                server_time: r.f64()?,
                reconnect_token: r.string()?,
                content_version: r.string()?,
                schema: EntitySchema::decode(&mut r)?,
            }),
            tag::DELTA => Self::Delta(decode_delta(&mut r, kinds)?),
            tag::EVENT => Self::Event {
                type_id: r.varuint()?,
                payload: r.bytes()?.to_vec(),
            },
            tag::PONG => Self::Pong {
                client_time: r.f64()?,
                server_time: r.f64()?,
            },
            t @ (tag::FREEZE | tag::RESUME) => {
                let notice = Notice {
                    reason: r.string()?,
                    version: r.string()?,
                    notes: r.string()?,
                };
                if t == tag::FREEZE {
                    Self::Freeze(notice)
                } else {
                    Self::Resume(notice)
                }
            }
            tag::HANDOFF => Self::Handoff {
                target_zone: r.string()?,
                transfer_ticket: r.string()?,
            },
            tag::KICK => Self::Kick {
                code: r.varuint()?,
                message: r.string()?,
            },
            tag::ERROR => Self::Error {
                code: r.varuint()?,
                message: r.string()?,
            },
            tag::REDIRECT => Self::Redirect {
                zone_id: r.string()?,
                url: r.string()?,
            },
            other => return Err(DecodeError::UnknownMessage(other)),
        };
        r.finish()?;
        Ok(message)
    }
}

fn kind_lookup(
    id: u32,
    created: &BTreeMap<u32, u16>,
    kinds: &dyn EntityKinds,
) -> Result<u16, DecodeError> {
    created
        .get(&id)
        .copied()
        .or_else(|| kinds.kind_of(id))
        .ok_or(DecodeError::UnknownEntity(id))
}

fn encode_delta(w: &mut Writer, d: &Delta, kinds: &dyn EntityKinds) -> Result<(), DecodeError> {
    let schema = kinds
        .schema()
        .ok_or(DecodeError::Invalid("delta needs a schema"))?;
    w.u8(tag::DELTA).varuint(d.server_tick).varuint(d.ack_seq);
    w.varuint(d.removes.len() as u32);
    for id in &d.removes {
        w.varuint(*id);
    }
    let mut created = BTreeMap::new();
    w.varuint(d.creates.len() as u32);
    for c in &d.creates {
        let kind = schema.get(c.kind).ok_or(DecodeError::UnknownEntity(c.id))?;
        if kind.fields.len() != c.fields.len() {
            return Err(DecodeError::Invalid("create must carry every field"));
        }
        w.varuint(c.id).varuint(u32::from(c.kind));
        for (def, value) in kind.fields.iter().zip(&c.fields) {
            def.ty.encode(w, value);
        }
        created.insert(c.id, c.kind);
    }
    w.varuint(d.updates.len() as u32);
    for u in &d.updates {
        let kind_id = kind_lookup(u.id, &created, kinds)?;
        let kind = schema
            .get(kind_id)
            .ok_or(DecodeError::UnknownEntity(u.id))?;
        w.varuint(u.id).varuint(u.mask);
        let mut values = u.fields.iter();
        for (index, def) in kind.fields.iter().enumerate() {
            if u.mask & (1 << index) != 0 {
                let value = values
                    .next()
                    .ok_or(DecodeError::Invalid("mask/field count mismatch"))?;
                def.ty.encode(w, value);
            }
        }
    }
    Ok(())
}

fn decode_delta(r: &mut Reader<'_>, kinds: &dyn EntityKinds) -> Result<Delta, DecodeError> {
    let schema = kinds
        .schema()
        .ok_or(DecodeError::Invalid("delta needs a schema"))?;
    let mut delta = Delta {
        server_tick: r.varuint()?,
        ack_seq: r.varuint()?,
        ..Delta::default()
    };
    for _ in 0..r.varuint()? {
        delta.removes.push(r.varuint()?);
    }
    let mut created = BTreeMap::new();
    for _ in 0..r.varuint()? {
        let id = r.varuint()?;
        let kind_id = u16::try_from(r.varuint()?).map_err(|_| DecodeError::Invalid("kind"))?;
        let kind = schema.get(kind_id).ok_or(DecodeError::UnknownEntity(id))?;
        let mut fields = Vec::with_capacity(kind.fields.len());
        for def in &kind.fields {
            fields.push(def.ty.decode(r)?);
        }
        created.insert(id, kind_id);
        delta.creates.push(EntityCreate {
            id,
            kind: kind_id,
            fields,
        });
    }
    for _ in 0..r.varuint()? {
        let id = r.varuint()?;
        let kind = schema
            .get(kind_lookup(id, &created, kinds)?)
            .ok_or(DecodeError::UnknownEntity(id))?;
        let mask = r.varuint()?;
        if kind.fields.len() < 32 && mask >> kind.fields.len() != 0 {
            return Err(DecodeError::Invalid("mask names a field the kind lacks"));
        }
        let mut fields = Vec::new();
        for (index, def) in kind.fields.iter().enumerate() {
            if mask & (1 << index) != 0 {
                fields.push(def.ty.decode(r)?);
            }
        }
        delta.updates.push(EntityUpdate { id, mask, fields });
    }
    Ok(delta)
}

/// Mirrored entity on a client.
#[derive(Debug, Clone, PartialEq)]
pub struct MirrorEntity {
    /// Kind id.
    pub kind: u16,
    /// Current decoded field values.
    pub fields: Vec<FieldValue>,
}

/// Reference client-side state: decodes server frames and applies Deltas.
///
/// This is the normative behaviour a client implementation (e.g. JS) must
/// match; it is also what the crate's own tests use.
#[derive(Debug, Clone, Default)]
pub struct ClientMirror {
    /// Last Welcome.
    pub welcome: Option<Welcome>,
    /// Known entities.
    pub entities: BTreeMap<u32, MirrorEntity>,
    /// Last Delta tick.
    pub server_tick: u32,
    /// Last acked input sequence.
    pub ack_seq: u32,
}

impl EntityKinds for ClientMirror {
    fn schema(&self) -> Option<&EntitySchema> {
        self.welcome.as_ref().map(|w| &w.schema)
    }
    fn kind_of(&self, id: u32) -> Option<u16> {
        self.entities.get(&id).map(|e| e.kind)
    }
}

impl ClientMirror {
    /// Empty mirror.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode a frame and apply it. Welcome resets all entities.
    pub fn apply(&mut self, frame: &[u8]) -> Result<ServerMessage, DecodeError> {
        let message = ServerMessage::decode(frame, self)?;
        match &message {
            ServerMessage::Welcome(w) => {
                self.welcome = Some(w.clone());
                self.entities.clear();
                self.server_tick = w.server_tick;
            }
            ServerMessage::Delta(d) => {
                for id in &d.removes {
                    self.entities.remove(id);
                }
                for c in &d.creates {
                    self.entities.insert(
                        c.id,
                        MirrorEntity {
                            kind: c.kind,
                            fields: c.fields.clone(),
                        },
                    );
                }
                for u in &d.updates {
                    let entity = self
                        .entities
                        .get_mut(&u.id)
                        .ok_or(DecodeError::UnknownEntity(u.id))?;
                    let mut values = u.fields.iter();
                    for (index, slot) in entity.fields.iter_mut().enumerate() {
                        if u.mask & (1 << index) != 0 {
                            if let Some(v) = values.next() {
                                *slot = v.clone();
                            }
                        }
                    }
                }
                self.server_tick = d.server_tick;
                self.ack_seq = d.ack_seq;
            }
            _ => {}
        }
        Ok(message)
    }
}
