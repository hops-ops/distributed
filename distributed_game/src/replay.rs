//! Deterministic record/replay.
//!
//! A [`Recorder`] wraps a [`ZoneHost`] and logs every external stimulus with
//! the clock reading it happened at: joins, reconnects, disconnects, inputs
//! (sequence, client tick, payload, outcome), kicks, freeze/resume, dispose,
//! and each `advance` together with the fact settlements delivered during it
//! (tagged with the tick they were delivered on) and a state hash after every
//! tick. Timers, RNG draws and tier changes are *not* logged: they are
//! derived from those inputs and the clock, so replay reproduces them.
//!
//! [`replay`] rebuilds a fresh host on a [`ManualClock`], feeds the log back
//! (settlements through a [`FactQueue::scripted`] queue so they land on the
//! same tick) and compares the per-tick hashes. The first mismatch is
//! returned as a [`Divergence`] whose `Display` shows the tick, the hashes
//! and the stimuli applied since the last matching advance.
//!
//! Requirements for a zone to replay: all randomness from
//! [`Ctx::rng`](crate::Ctx::rng), all time from `ctx.dt()` / `ctx.sim_time()`
//! (never `ctx.now()` or wall time), no iteration over `HashMap`s that feeds
//! state. Override [`Zone::state_hash`] to cover unreplicated state.
//!
//! The file is compact binary (`DGR1`, varints; see [`Recording::encode`]).
//! Reconnect tokens are stored as indices into the host's token sequence,
//! never as secrets. Zone construction parameters are opaque bytes chosen by
//! the caller ([`Recording::zone_config`]); [`replay`] takes the decoded
//! `Z::Config`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::clock::{Clock, ManualClock};
use crate::facts::{FactError, FactOutcome, FactQueue, FactReceipt, FactSettlement};
use crate::host::{AdvanceReport, HostConfig, JoinError, Joined, Outbound, ZoneHost};
use crate::input::PushOutcome;
use crate::interest::InterestConfig;
use crate::protocol::wire::{DecodeError, Reader, Writer};
use crate::protocol::{Notice, Role};
use crate::replication::WorldView;
use crate::rewind::RewindConfig;
use crate::rng::Fnv64;
use crate::schema::FieldValue;
use crate::zone::{ClientId, PlayerId, TicketClaims, Zone};

const MAGIC: &[u8; 4] = b"DGR1";

/// FNV digest of a replicated world (entity order, ids, kinds, positions,
/// priorities and field values).
pub fn hash_world(view: &WorldView) -> u64 {
    let mut h = Fnv64::new();
    for e in view.entities() {
        h.write_u64(u64::from(e.id))
            .write_u64(u64::from(e.kind))
            .write_f32(e.x)
            .write_f32(e.y)
            .write_f32(e.priority)
            .write(&[u8::from(e.global)]);
        for f in &e.fields {
            match f {
                FieldValue::Bool(v) => h.write(&[0, u8::from(*v)]),
                FieldValue::U(v) => h.write(&[1]).write_u64(u64::from(*v)),
                FieldValue::I(v) => h.write(&[2]).write(&v.to_le_bytes()),
                FieldValue::F(v) => h.write(&[3]).write_f32(*v),
                FieldValue::Str(v) => h.write(&[4]).write_u64(v.len() as u64).write(v.as_bytes()),
                FieldValue::Bytes(v) => h.write(&[5]).write_u64(v.len() as u64).write(v),
            };
        }
    }
    h.finish()
}

/// Result of a recorded join/reconnect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinResult {
    /// Admitted on this connection id.
    Joined(u32),
    /// Refused with this error code.
    Refused(u32),
}

impl From<&Result<Joined, JoinError>> for JoinResult {
    fn from(r: &Result<Joined, JoinError>) -> Self {
        match r {
            Ok(j) => Self::Joined(j.client.0),
            Err(e) => Self::Refused(e.code),
        }
    }
}

/// One recorded stimulus. `at` is the host clock when it happened.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// [`ZoneHost::join`].
    Join {
        /// Clock.
        at: Duration,
        /// Verified claims.
        claims: TicketClaims,
        /// Hello capabilities.
        capabilities: u32,
        /// Client content version.
        content_version: String,
        /// Outcome.
        result: JoinResult,
    },
    /// [`ZoneHost::reconnect`].
    Reconnect {
        /// Clock.
        at: Duration,
        /// Index of the token in the host's token sequence (`u32::MAX` = unknown).
        token: u32,
        /// Hello capabilities.
        capabilities: u32,
        /// Outcome.
        result: JoinResult,
    },
    /// [`ZoneHost::disconnect`].
    Disconnect {
        /// Clock.
        at: Duration,
        /// Connection.
        client: u32,
        /// Leave vs drop.
        graceful: bool,
    },
    /// [`ZoneHost::push_input`].
    Input {
        /// Clock.
        at: Duration,
        /// Connection.
        client: u32,
        /// Sequence.
        seq: u32,
        /// Client tick.
        client_tick: u32,
        /// Raw payload.
        payload: Vec<u8>,
        /// Outcome.
        outcome: PushOutcome,
    },
    /// [`ZoneHost::kick`].
    Kick {
        /// Clock.
        at: Duration,
        /// Player.
        player: u32,
        /// Message.
        message: String,
    },
    /// [`ZoneHost::freeze`].
    Freeze {
        /// Clock.
        at: Duration,
        /// Notice.
        notice: Notice,
    },
    /// [`ZoneHost::resume`].
    Resume {
        /// Clock.
        at: Duration,
        /// Notice.
        notice: Notice,
    },
    /// [`ZoneHost::dispose`].
    Dispose {
        /// Clock.
        at: Duration,
    },
    /// [`ZoneHost::advance`].
    Advance {
        /// Clock.
        at: Duration,
        /// Settlements delivered, with their delivery tick.
        settled: Vec<(u64, FactSettlement)>,
        /// `(tick, hash)` after each tick simulated.
        hashes: Vec<(u64, u64)>,
    },
}

impl Event {
    /// Clock reading.
    pub fn at(&self) -> Duration {
        match self {
            Self::Join { at, .. }
            | Self::Reconnect { at, .. }
            | Self::Disconnect { at, .. }
            | Self::Input { at, .. }
            | Self::Kick { at, .. }
            | Self::Freeze { at, .. }
            | Self::Resume { at, .. }
            | Self::Dispose { at }
            | Self::Advance { at, .. } => *at,
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = self.at().as_secs_f64() * 1000.0;
        match self {
            Self::Join {
                claims, result, ..
            } => write!(f, "@{ms:.1}ms join {} -> {result:?}", claims.principal),
            Self::Reconnect { token, result, .. } => {
                write!(f, "@{ms:.1}ms reconnect token#{token} -> {result:?}")
            }
            Self::Disconnect {
                client, graceful, ..
            } => write!(f, "@{ms:.1}ms disconnect client {client} graceful={graceful}"),
            Self::Input {
                client,
                seq,
                client_tick,
                payload,
                outcome,
                ..
            } => write!(
                f,
                "@{ms:.1}ms input client {client} seq {seq} client_tick {client_tick} payload {payload:02x?} -> {outcome:?}"
            ),
            Self::Kick {
                player, message, ..
            } => write!(f, "@{ms:.1}ms kick player {player}: {message}"),
            Self::Freeze { notice, .. } => write!(f, "@{ms:.1}ms freeze {}", notice.reason),
            Self::Resume { notice, .. } => write!(f, "@{ms:.1}ms resume {}", notice.reason),
            Self::Dispose { .. } => write!(f, "@{ms:.1}ms dispose"),
            Self::Advance {
                settled, hashes, ..
            } => {
                let ticks = match (hashes.first(), hashes.last()) {
                    (Some(a), Some(b)) => format!("ticks {}..={}", a.0, b.0),
                    _ => "no ticks".into(),
                };
                write!(f, "@{ms:.1}ms advance {ticks}, {} settled", settled.len())
            }
        }
    }
}

/// A recorded session.
#[derive(Debug, Clone, PartialEq)]
pub struct Recording {
    /// Zone id (seeds the RNG).
    pub zone_id: String,
    /// Host configuration.
    pub config: HostConfig,
    /// Clock when the host was created.
    pub created_at: Duration,
    /// Fact queue capacity, when a queue was attached.
    pub fact_capacity: Option<usize>,
    /// Caller-encoded zone construction parameters (opaque).
    pub zone_config: Vec<u8>,
    /// Stimuli in order.
    pub events: Vec<Event>,
}

/// File decode failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingError {
    /// Not a `DGR1` file.
    BadMagic,
    /// Malformed content.
    Decode(String),
}

impl fmt::Display for RecordingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => write!(f, "not a distributed_game recording"),
            Self::Decode(e) => write!(f, "malformed recording: {e}"),
        }
    }
}

impl std::error::Error for RecordingError {}

impl From<DecodeError> for RecordingError {
    fn from(e: DecodeError) -> Self {
        Self::Decode(e.to_string())
    }
}

fn bad(what: &'static str) -> RecordingError {
    RecordingError::Decode(what.into())
}

fn put_u64(w: &mut Writer, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            w.u8(byte);
            return;
        }
        w.u8(byte | 0x80);
    }
}

fn get_u64(r: &mut Reader<'_>) -> Result<u64, RecordingError> {
    let mut v = 0u64;
    for i in 0..10 {
        let byte = r.u8()?;
        v |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(bad("u64 varint overflow"))
}

fn put_dur(w: &mut Writer, d: Duration) {
    put_u64(w, d.as_nanos() as u64);
}

fn get_dur(r: &mut Reader<'_>) -> Result<Duration, RecordingError> {
    Ok(Duration::from_nanos(get_u64(r)?))
}

fn put_notice(w: &mut Writer, n: &Notice) {
    w.string(&n.reason).string(&n.version).string(&n.notes);
}

fn get_notice(r: &mut Reader<'_>) -> Result<Notice, RecordingError> {
    Ok(Notice {
        reason: r.string()?,
        version: r.string()?,
        notes: r.string()?,
    })
}

fn put_join_result(w: &mut Writer, j: &JoinResult) {
    match j {
        JoinResult::Joined(c) => w.u8(0).varuint(*c),
        JoinResult::Refused(c) => w.u8(1).varuint(*c),
    };
}

fn get_join_result(r: &mut Reader<'_>) -> Result<JoinResult, RecordingError> {
    match r.u8()? {
        0 => Ok(JoinResult::Joined(r.varuint()?)),
        1 => Ok(JoinResult::Refused(r.varuint()?)),
        _ => Err(bad("join result")),
    }
}

const OUTCOMES: [PushOutcome; 6] = [
    PushOutcome::Accepted,
    PushOutcome::Duplicate,
    PushOutcome::Stale,
    PushOutcome::AcceptedDroppedOldest,
    PushOutcome::Rejected,
    PushOutcome::UnknownClient,
];

fn put_settlement(w: &mut Writer, tick: u64, s: &FactSettlement) {
    put_u64(w, tick);
    w.string(&s.key).string(&s.command);
    match &s.outcome {
        FactOutcome::Committed(receipt) => {
            w.u8(0)
                .string(&receipt.payload.to_string())
                .bool(receipt.replayed);
        }
        FactOutcome::Failed { error, attempts } => {
            let (kind, msg) = match error {
                FactError::Retryable(m) => (0, m),
                FactError::Permanent(m) => (1, m),
            };
            w.u8(1).u8(kind).string(msg).varuint(*attempts);
        }
    }
}

fn get_settlement(r: &mut Reader<'_>) -> Result<(u64, FactSettlement), RecordingError> {
    let tick = get_u64(r)?;
    let key = r.string()?;
    let command = r.string()?;
    let outcome = match r.u8()? {
        0 => {
            let payload = serde_json::from_str(&r.string()?).map_err(|_| bad("receipt json"))?;
            FactOutcome::Committed(FactReceipt {
                payload,
                replayed: r.bool()?,
            })
        }
        1 => {
            let kind = r.u8()?;
            let msg = r.string()?;
            let error = match kind {
                0 => FactError::Retryable(msg),
                1 => FactError::Permanent(msg),
                _ => return Err(bad("fact error kind")),
            };
            FactOutcome::Failed {
                error,
                attempts: r.varuint()?,
            }
        }
        _ => return Err(bad("fact outcome")),
    };
    Ok((
        tick,
        FactSettlement {
            key,
            command,
            outcome,
        },
    ))
}

fn put_config(w: &mut Writer, c: &HostConfig) {
    w.varuint(c.tick_rate).varuint(c.patch_rate);
    match c.idle_tick_rate {
        Some(hz) => w.u8(1).varuint(hz),
        None => w.u8(0),
    };
    put_dur(w, c.dormant_after);
    w.varuint(c.max_catch_up);
    put_u64(w, c.max_players as u64);
    put_u64(w, c.max_observers as u64);
    put_u64(w, c.input_capacity as u64);
    put_dur(w, c.reconnect_grace);
    put_u64(w, c.byte_budget as u64);
    w.f32(c.default_view_radius)
        .f32(c.interest.cell_size)
        .f32(c.interest.leave_margin)
        .string(&c.content_version);
    put_u64(w, c.world_seed);
    put_dur(w, c.rewind.max_rewind);
    put_u64(w, c.rewind.max_frames as u64);
}

fn get_config(r: &mut Reader<'_>) -> Result<HostConfig, RecordingError> {
    Ok(HostConfig {
        tick_rate: r.varuint()?,
        patch_rate: r.varuint()?,
        idle_tick_rate: match r.u8()? {
            0 => None,
            _ => Some(r.varuint()?),
        },
        dormant_after: get_dur(r)?,
        max_catch_up: r.varuint()?,
        max_players: get_u64(r)? as usize,
        max_observers: get_u64(r)? as usize,
        input_capacity: get_u64(r)? as usize,
        reconnect_grace: get_dur(r)?,
        byte_budget: get_u64(r)? as usize,
        default_view_radius: r.f32()?,
        interest: InterestConfig {
            cell_size: r.f32()?,
            leave_margin: r.f32()?,
        },
        content_version: r.string()?,
        world_seed: get_u64(r)?,
        rewind: RewindConfig {
            max_rewind: get_dur(r)?,
            max_frames: get_u64(r)? as usize,
        },
    })
}

impl Recording {
    /// Compact binary encoding:
    /// `"DGR1" ‖ zone_id ‖ config ‖ created_at ‖ fact_capacity? ‖ zone_config ‖
    /// varuint(event count) ‖ events`. Integers are LEB128 varints,
    /// durations are nanosecond varints, clock readings are delta-coded
    /// against the previous event, hashes are 8 raw bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.raw(MAGIC).string(&self.zone_id);
        put_config(&mut w, &self.config);
        put_dur(&mut w, self.created_at);
        match self.fact_capacity {
            Some(c) => {
                w.u8(1);
                put_u64(&mut w, c as u64);
            }
            None => {
                w.u8(0);
            }
        }
        put_u64(&mut w, self.zone_config.len() as u64);
        w.raw(&self.zone_config);
        put_u64(&mut w, self.events.len() as u64);
        let mut last = self.created_at;
        for event in &self.events {
            let at = event.at();
            put_dur(&mut w, at.saturating_sub(last));
            last = at;
            match event {
                Event::Join {
                    claims,
                    capabilities,
                    content_version,
                    result,
                    ..
                } => {
                    w.u8(0)
                        .string(&claims.principal)
                        .string(&claims.zone)
                        .string(&claims.nonce);
                    put_u64(&mut w, claims.expires_at);
                    w.u8(match claims.role {
                        Role::Player => 0,
                        Role::Observer => 1,
                    })
                    .varuint(*capabilities)
                    .string(content_version);
                    put_join_result(&mut w, result);
                }
                Event::Reconnect {
                    token,
                    capabilities,
                    result,
                    ..
                } => {
                    w.u8(1).varuint(*token).varuint(*capabilities);
                    put_join_result(&mut w, result);
                }
                Event::Disconnect {
                    client, graceful, ..
                } => {
                    w.u8(2).varuint(*client).bool(*graceful);
                }
                Event::Input {
                    client,
                    seq,
                    client_tick,
                    payload,
                    outcome,
                    ..
                } => {
                    let code = OUTCOMES.iter().position(|o| o == outcome).unwrap_or(0) as u8;
                    w.u8(3)
                        .varuint(*client)
                        .varuint(*seq)
                        .varuint(*client_tick)
                        .bytes(payload)
                        .u8(code);
                }
                Event::Kick {
                    player, message, ..
                } => {
                    w.u8(4).varuint(*player).string(message);
                }
                Event::Freeze { notice, .. } => {
                    w.u8(5);
                    put_notice(&mut w, notice);
                }
                Event::Resume { notice, .. } => {
                    w.u8(6);
                    put_notice(&mut w, notice);
                }
                Event::Dispose { .. } => {
                    w.u8(7);
                }
                Event::Advance {
                    settled, hashes, ..
                } => {
                    w.u8(8);
                    put_u64(&mut w, settled.len() as u64);
                    for (tick, s) in settled {
                        put_settlement(&mut w, *tick, s);
                    }
                    put_u64(&mut w, hashes.len() as u64);
                    if let Some((first, _)) = hashes.first() {
                        put_u64(&mut w, *first);
                    }
                    for (_, hash) in hashes {
                        w.raw(&hash.to_le_bytes());
                    }
                }
            }
        }
        w.into_bytes()
    }

    /// Decode [`encode`](Self::encode) output.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecordingError> {
        let mut r = Reader::new(bytes);
        if r.raw(4).map_err(|_| RecordingError::BadMagic)? != MAGIC {
            return Err(RecordingError::BadMagic);
        }
        let zone_id = r.string()?;
        let config = get_config(&mut r)?;
        let created_at = get_dur(&mut r)?;
        let fact_capacity = match r.u8()? {
            0 => None,
            _ => Some(get_u64(&mut r)? as usize),
        };
        let len = get_u64(&mut r)? as usize;
        let zone_config = r.raw(len)?.to_vec();
        let count = get_u64(&mut r)? as usize;
        let mut events = Vec::with_capacity(count.min(1 << 20));
        let mut at = created_at;
        for _ in 0..count {
            at += get_dur(&mut r)?;
            let event = match r.u8()? {
                0 => {
                    let principal = r.string()?;
                    let zone = r.string()?;
                    let nonce = r.string()?;
                    let expires_at = get_u64(&mut r)?;
                    let role = match r.u8()? {
                        0 => Role::Player,
                        _ => Role::Observer,
                    };
                    Event::Join {
                        at,
                        claims: TicketClaims {
                            principal,
                            zone,
                            expires_at,
                            nonce,
                            role,
                        },
                        capabilities: r.varuint()?,
                        content_version: r.string()?,
                        result: get_join_result(&mut r)?,
                    }
                }
                1 => Event::Reconnect {
                    at,
                    token: r.varuint()?,
                    capabilities: r.varuint()?,
                    result: get_join_result(&mut r)?,
                },
                2 => Event::Disconnect {
                    at,
                    client: r.varuint()?,
                    graceful: r.bool()?,
                },
                3 => Event::Input {
                    at,
                    client: r.varuint()?,
                    seq: r.varuint()?,
                    client_tick: r.varuint()?,
                    payload: r.bytes()?.to_vec(),
                    outcome: *OUTCOMES.get(r.u8()? as usize).ok_or(bad("input outcome"))?,
                },
                4 => Event::Kick {
                    at,
                    player: r.varuint()?,
                    message: r.string()?,
                },
                5 => Event::Freeze {
                    at,
                    notice: get_notice(&mut r)?,
                },
                6 => Event::Resume {
                    at,
                    notice: get_notice(&mut r)?,
                },
                7 => Event::Dispose { at },
                8 => {
                    let n = get_u64(&mut r)? as usize;
                    let mut settled = Vec::with_capacity(n.min(1 << 16));
                    for _ in 0..n {
                        settled.push(get_settlement(&mut r)?);
                    }
                    let n = get_u64(&mut r)?;
                    let mut hashes = Vec::with_capacity((n as usize).min(1 << 16));
                    if n > 0 {
                        let first = get_u64(&mut r)?;
                        for i in 0..n {
                            let mut b = [0u8; 8];
                            b.copy_from_slice(r.raw(8)?);
                            hashes.push((first + i, u64::from_le_bytes(b)));
                        }
                    }
                    Event::Advance {
                        at,
                        settled,
                        hashes,
                    }
                }
                _ => return Err(bad("event tag")),
            };
            events.push(event);
        }
        r.finish()?;
        Ok(Self {
            zone_id,
            config,
            created_at,
            fact_capacity,
            zone_config,
            events,
        })
    }

    /// Write [`encode`](Self::encode) output to `path`.
    pub fn save(&self, path: impl AsRef<std::path::Path>) -> std::io::Result<()> {
        std::fs::write(path, self.encode())
    }

    /// Read a file written by [`save`](Self::save).
    pub fn load(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::decode(&bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// Simulated ticks in the recording.
    pub fn tick_count(&self) -> usize {
        self.events
            .iter()
            .map(|e| match e {
                Event::Advance { hashes, .. } => hashes.len(),
                _ => 0,
            })
            .sum()
    }
}

/// Records a session while driving a [`ZoneHost`]. Use its methods instead
/// of the host's so every stimulus is logged.
pub struct Recorder<Z: Zone> {
    host: ZoneHost<Z>,
    clock: Arc<dyn Clock>,
    recording: Recording,
    tokens: Arc<Mutex<Vec<String>>>,
}

impl<Z: Zone> Recorder<Z> {
    /// Create the host (as [`ZoneHost::new`]) and start recording.
    /// `zone_config_bytes` is stored verbatim for whoever replays.
    pub fn new(
        zone_id: impl Into<String>,
        config: HostConfig,
        clock: Arc<dyn Clock>,
        zone_config: Z::Config,
        zone_config_bytes: Vec<u8>,
    ) -> Self {
        let zone_id = zone_id.into();
        let created_at = clock.now();
        let tokens = Arc::new(Mutex::new(Vec::new()));
        let sink = tokens.clone();
        let mut host = ZoneHost::new(zone_id.clone(), config.clone(), clock.clone(), zone_config)
            .with_token_source(move || {
                let token = crate::host::default_token();
                sink.lock().expect("tokens").push(token.clone());
                token
            });
        host.set_tick_hashing(true);
        Self {
            host,
            clock,
            recording: Recording {
                zone_id,
                config,
                created_at,
                fact_capacity: None,
                zone_config: zone_config_bytes,
                events: Vec::new(),
            },
            tokens,
        }
    }

    /// Attach a live fact queue (as [`ZoneHost::with_facts`]); `capacity`
    /// must be the capacity it was created with.
    pub fn with_facts(mut self, facts: FactQueue, capacity: usize) -> Self {
        self.host = self.host.with_facts(facts);
        self.recording.fact_capacity = Some(capacity);
        self
    }

    /// The host (read-only: mutations bypassing the recorder break replay).
    pub fn host(&self) -> &ZoneHost<Z> {
        &self.host
    }

    /// The recording so far.
    pub fn recording(&self) -> &Recording {
        &self.recording
    }

    /// Stop and return the recording.
    pub fn finish(self) -> Recording {
        self.recording
    }

    fn now(&self) -> Duration {
        self.clock.now()
    }

    /// [`ZoneHost::join`].
    pub fn join(
        &mut self,
        claims: &TicketClaims,
        capabilities: u32,
        content_version: &str,
    ) -> Result<Joined, JoinError> {
        let at = self.now();
        let result = self.host.join(claims, capabilities, content_version);
        self.recording.events.push(Event::Join {
            at,
            claims: claims.clone(),
            capabilities,
            content_version: content_version.into(),
            result: (&result).into(),
        });
        result
    }

    /// [`ZoneHost::reconnect`].
    pub fn reconnect(&mut self, token: &str, capabilities: u32) -> Result<Joined, JoinError> {
        let at = self.now();
        let index = self
            .tokens
            .lock()
            .expect("tokens")
            .iter()
            .position(|t| t == token)
            .map_or(u32::MAX, |i| i as u32);
        let result = self.host.reconnect(token, capabilities);
        self.recording.events.push(Event::Reconnect {
            at,
            token: index,
            capabilities,
            result: (&result).into(),
        });
        result
    }

    /// [`ZoneHost::disconnect`].
    pub fn disconnect(&mut self, client: ClientId, graceful: bool) {
        let at = self.now();
        self.host.disconnect(client, graceful);
        self.recording.events.push(Event::Disconnect {
            at,
            client: client.0,
            graceful,
        });
    }

    /// [`ZoneHost::push_input`].
    pub fn push_input(
        &mut self,
        client: ClientId,
        seq: u32,
        client_tick: u32,
        payload: &[u8],
    ) -> PushOutcome {
        let at = self.now();
        let outcome = self.host.push_input(client, seq, client_tick, payload);
        self.recording.events.push(Event::Input {
            at,
            client: client.0,
            seq,
            client_tick,
            payload: payload.to_vec(),
            outcome,
        });
        outcome
    }

    /// [`ZoneHost::kick`].
    pub fn kick(&mut self, player: PlayerId, message: &str) {
        let at = self.now();
        self.host.kick(player, message);
        self.recording.events.push(Event::Kick {
            at,
            player: player.0,
            message: message.into(),
        });
    }

    /// [`ZoneHost::freeze`].
    pub fn freeze(&mut self, notice: Notice) {
        let at = self.now();
        self.host.freeze(notice.clone());
        self.recording.events.push(Event::Freeze { at, notice });
    }

    /// [`ZoneHost::resume`].
    pub fn resume(&mut self, notice: Notice) {
        let at = self.now();
        self.host.resume(notice.clone());
        self.recording.events.push(Event::Resume { at, notice });
    }

    /// [`ZoneHost::dispose`].
    pub fn dispose(&mut self) {
        let at = self.now();
        self.host.dispose();
        self.recording.events.push(Event::Dispose { at });
    }

    /// [`ZoneHost::advance`].
    pub fn advance(&mut self) -> AdvanceReport {
        let at = self.now();
        let report = self.host.advance();
        self.recording.events.push(Event::Advance {
            at,
            settled: report.settled.clone(),
            hashes: report.tick_hashes.clone(),
        });
        report
    }

    /// [`ZoneHost::drain_outbound`].
    pub fn drain_outbound(&mut self) -> Vec<Outbound> {
        self.host.drain_outbound()
    }
}

/// What diverged.
#[derive(Debug, Clone, PartialEq)]
pub enum DivergenceKind {
    /// State hash after a tick differs.
    StateHash {
        /// Recorded.
        expected: u64,
        /// Replayed.
        actual: u64,
    },
    /// The advance simulated different ticks.
    Ticks {
        /// Recorded ticks.
        expected: Vec<u64>,
        /// Replayed ticks.
        actual: Vec<u64>,
    },
    /// An input was accepted/rejected differently.
    InputOutcome {
        /// Recorded.
        expected: PushOutcome,
        /// Replayed.
        actual: PushOutcome,
    },
    /// A join/reconnect resolved differently.
    Join {
        /// Recorded.
        expected: JoinResult,
        /// Replayed.
        actual: JoinResult,
    },
}

/// First point where a replay departed from its recording.
#[derive(Debug, Clone, PartialEq)]
pub struct Divergence {
    /// Index into [`Recording::events`].
    pub event_index: usize,
    /// Tick at which it was detected (the diverging tick for hashes).
    pub tick: u64,
    /// What differed.
    pub kind: DivergenceKind,
    /// Last matching tick (0 = none).
    pub last_good_tick: u64,
    /// Stimuli applied since the last matching advance (most likely cause).
    pub suspects: Vec<String>,
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            DivergenceKind::StateHash { expected, actual } => write!(
                f,
                "replay diverged at tick {} (event #{}): state hash {actual:016x} != recorded {expected:016x}",
                self.tick, self.event_index
            )?,
            DivergenceKind::Ticks { expected, actual } => write!(
                f,
                "replay diverged at tick {} (event #{}): simulated ticks {actual:?} != recorded {expected:?}",
                self.tick, self.event_index
            )?,
            DivergenceKind::InputOutcome { expected, actual } => write!(
                f,
                "replay diverged at tick {} (event #{}): input outcome {actual:?} != recorded {expected:?}",
                self.tick, self.event_index
            )?,
            DivergenceKind::Join { expected, actual } => write!(
                f,
                "replay diverged at tick {} (event #{}): join {actual:?} != recorded {expected:?}",
                self.tick, self.event_index
            )?,
        }
        write!(f, "\n  last matching tick: {}", self.last_good_tick)?;
        if !self.suspects.is_empty() {
            write!(f, "\n  stimuli since then:")?;
            for s in &self.suspects {
                write!(f, "\n    {s}")?;
            }
        }
        Ok(())
    }
}

impl std::error::Error for Divergence {}

/// Successful replay summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySummary {
    /// Events applied.
    pub events: usize,
    /// Ticks simulated and verified.
    pub ticks: u64,
    /// Hash after the last tick.
    pub final_hash: u64,
}

/// Replay `recording` through a fresh host on a [`ManualClock`] and verify
/// every per-tick state hash.
pub fn replay<Z: Zone>(
    recording: &Recording,
    zone_config: Z::Config,
) -> Result<ReplaySummary, Divergence> {
    replay_with::<Z>(recording, zone_config, |_| {})
}

/// [`replay`] with a callback after each event (inspection, tooling).
pub fn replay_with<Z: Zone>(
    recording: &Recording,
    zone_config: Z::Config,
    mut inspect: impl FnMut(&ZoneHost<Z>),
) -> Result<ReplaySummary, Divergence> {
    let clock = ManualClock::new();
    clock.set(recording.created_at);
    let counter = Arc::new(Mutex::new(0u32));
    let source = counter.clone();
    let mut host = ZoneHost::<Z>::new(
        recording.zone_id.clone(),
        recording.config.clone(),
        Arc::new(clock.clone()),
        zone_config,
    )
    .with_token_source(move || {
        let mut n = source.lock().expect("counter");
        *n += 1;
        format!("replay-token-{}", *n - 1)
    });
    host.set_tick_hashing(true);
    let mut script = None;
    if let Some(capacity) = recording.fact_capacity {
        let (queue, s) = FactQueue::scripted(capacity);
        host = host.with_facts(queue);
        script = Some(s);
    }
    let mut last_good_tick = 0u64;
    let mut suspects: Vec<String> = Vec::new();
    let mut last_hash = 0u64;
    let mut ticks = 0u64;
    let diverge = |event_index: usize,
                   tick: u64,
                   kind: DivergenceKind,
                   last_good_tick: u64,
                   suspects: &[String]| Divergence {
        event_index,
        tick,
        kind,
        last_good_tick,
        suspects: suspects.to_vec(),
    };
    for (index, event) in recording.events.iter().enumerate() {
        clock.set(event.at());
        match event {
            Event::Join {
                claims,
                capabilities,
                content_version,
                result,
                ..
            } => {
                let actual = JoinResult::from(&host.join(claims, *capabilities, content_version));
                if &actual != result {
                    return Err(diverge(
                        index,
                        host.tick(),
                        DivergenceKind::Join {
                            expected: result.clone(),
                            actual,
                        },
                        last_good_tick,
                        &suspects,
                    ));
                }
            }
            Event::Reconnect {
                token,
                capabilities,
                result,
                ..
            } => {
                let token = if *token == u32::MAX {
                    String::new()
                } else {
                    format!("replay-token-{token}")
                };
                let actual = JoinResult::from(&host.reconnect(&token, *capabilities));
                if &actual != result {
                    return Err(diverge(
                        index,
                        host.tick(),
                        DivergenceKind::Join {
                            expected: result.clone(),
                            actual,
                        },
                        last_good_tick,
                        &suspects,
                    ));
                }
            }
            Event::Disconnect {
                client, graceful, ..
            } => host.disconnect(ClientId(*client), *graceful),
            Event::Input {
                client,
                seq,
                client_tick,
                payload,
                outcome,
                ..
            } => {
                let actual = host.push_input(ClientId(*client), *seq, *client_tick, payload);
                if actual != *outcome {
                    return Err(diverge(
                        index,
                        host.tick(),
                        DivergenceKind::InputOutcome {
                            expected: *outcome,
                            actual,
                        },
                        last_good_tick,
                        &suspects,
                    ));
                }
            }
            Event::Kick {
                player, message, ..
            } => host.kick(PlayerId(*player), message.clone()),
            Event::Freeze { notice, .. } => host.freeze(notice.clone()),
            Event::Resume { notice, .. } => host.resume(notice.clone()),
            Event::Dispose { .. } => host.dispose(),
            Event::Advance {
                settled, hashes, ..
            } => {
                if let Some(script) = &script {
                    for (tick, s) in settled {
                        script.push(*tick, s.clone());
                    }
                }
                let report = host.advance();
                host.drain_outbound();
                let expected_ticks: Vec<u64> = hashes.iter().map(|(t, _)| *t).collect();
                let actual_ticks: Vec<u64> = report.tick_hashes.iter().map(|(t, _)| *t).collect();
                for ((tick, expected), (_, actual)) in hashes.iter().zip(&report.tick_hashes) {
                    if expected != actual {
                        suspects.push(event.to_string());
                        return Err(diverge(
                            index,
                            *tick,
                            DivergenceKind::StateHash {
                                expected: *expected,
                                actual: *actual,
                            },
                            last_good_tick,
                            &suspects,
                        ));
                    }
                    last_good_tick = *tick;
                }
                if expected_ticks != actual_ticks {
                    suspects.push(event.to_string());
                    return Err(diverge(
                        index,
                        host.tick(),
                        DivergenceKind::Ticks {
                            expected: expected_ticks,
                            actual: actual_ticks,
                        },
                        last_good_tick,
                        &suspects,
                    ));
                }
                if let Some((_, h)) = report.tick_hashes.last() {
                    last_hash = *h;
                    ticks += report.tick_hashes.len() as u64;
                    suspects.clear();
                }
                inspect(&host);
                continue;
            }
        }
        suspects.push(event.to_string());
        host.drain_outbound();
        inspect(&host);
    }
    Ok(ReplaySummary {
        events: recording.events.len(),
        ticks,
        final_hash: last_hash,
    })
}

/// Per-tick hashes of a recording (for diffing two recordings).
pub fn tick_hashes(recording: &Recording) -> BTreeMap<u64, u64> {
    recording
        .events
        .iter()
        .flat_map(|e| match e {
            Event::Advance { hashes, .. } => hashes.clone(),
            _ => Vec::new(),
        })
        .collect()
}
