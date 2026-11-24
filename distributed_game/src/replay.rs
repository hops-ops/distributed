//! Deterministic record/replay.
//!
//! A host created with [`HostConfig::record`] keeps every [`Stimulus`] it
//! is [applied](ZoneHost::apply) — joins, reconnects, disconnects, inputs,
//! content patches, focus, inbox kicks and messages, zone messages, child
//! events, freeze/resume, version switches, reloads, kicks, handoffs,
//! restores, dispose and each advance — whichever driver applies it (the
//! native server, a celld zone object, a test). Each [`Entry`] carries the
//! clock reading the stimulus ran at (monotonic and wall: the host freezes
//! both for the whole stimulus, so every time the zone and its fact stamps
//! see is in the recording) and what it observed: join and input outcomes,
//! and for an advance the fact settlements delivered (tagged with their
//! tick) and a state hash after every tick. Timers, RNG draws and tier
//! changes are *not* logged: they are derived from those inputs and the
//! clock, so replay reproduces them. Handoffs issued by the zone replay
//! against a stub issuer that always signs (the ticket text is output, not
//! state); a recording made with an issuer that *refused* a handoff will
//! diverge.
//!
//! [`replay`] rebuilds a fresh host on the recorded clock readings, applies
//! the entries back (settlements through a [`FactQueue::scripted`] queue so
//! they land on the same tick) and compares join and input outcomes and the
//! per-tick hashes. The first mismatch is returned as a [`Divergence`]
//! whose `Display` shows the tick, the hashes and the stimuli applied since
//! the last matching advance. A successful replay returns the facts the
//! zone emitted ([`ReplaySummary::facts`]): the same keys, inputs,
//! principals and stamps as the live run's.
//!
//! Requirements for a zone to replay: all randomness from
//! [`Ctx::rng`](crate::Ctx::rng), all time from the [`Ctx`](crate::Ctx)
//! (never wall time read directly), no iteration over `HashMap`s that feeds
//! state. Override [`Zone::state_hash`] to cover unreplicated state.
//!
//! The file is compact binary (`DGR4`, varints; see [`Recording::encode`]).
//! Reconnect tokens are stored as their place in the host's token sequence
//! (`token#N`), never as secrets. [`replay`] takes the zone's `Z::Config`.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::clock::Clock;
use crate::facts::{Fact, FactError, FactOutcome, FactQueue, FactReceipt, FactSettlement};
use crate::handoff::{HandoffRequest, Transfer};
use crate::host::{HostConfig, JoinError, Joined, Outcome, Stimulus, ZoneHost};
use crate::input::PushOutcome;
use crate::interest::InterestConfig;
use crate::protocol::wire::{DecodeError, Reader, Writer};
use crate::protocol::{Notice, Role};
use crate::replication::WorldView;
use crate::rewind::RewindConfig;
use crate::rng::Fnv64;
use crate::schema::FieldValue;
use crate::world::{ChildEvent, StalePolicy};
use crate::zone::{ClientId, PlayerId, TicketClaims, Zone};
use crate::zone_message::ZoneEnvelope;

const MAGIC: &[u8; 4] = b"DGR4";

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

/// What a recorded stimulus observed that replay must reproduce.
#[derive(Debug, Clone, PartialEq)]
pub enum Observed {
    /// Nothing checked.
    Nothing,
    /// A join or reconnect.
    Join(JoinResult),
    /// An input.
    Input(PushOutcome),
    /// An advance.
    Advance {
        /// Settlements delivered, with their delivery tick (the fact
        /// sink's answers: replayed, not recomputed).
        settled: Vec<(u64, FactSettlement)>,
        /// `(tick, hash)` after each tick simulated.
        hashes: Vec<(u64, u64)>,
    },
}

impl Observed {
    pub(crate) fn of(outcome: &Outcome) -> Self {
        match outcome {
            Outcome::Joined(result) => Self::Join(result.into()),
            Outcome::Input(outcome) => Self::Input(*outcome),
            Outcome::Advanced(report) => Self::Advance {
                settled: report.settled.clone(),
                hashes: report.tick_hashes.clone(),
            },
            _ => Self::Nothing,
        }
    }
}

/// One recorded stimulus.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// Host clock when it ran.
    pub at: Duration,
    /// Wall time (Unix ms) when it ran.
    pub unix_ms: u64,
    /// The stimulus (a reconnect's token as `token#N`).
    pub stimulus: Stimulus,
    /// What it observed.
    pub observed: Observed,
}

/// The recorded name of the `n`-th token a host issued.
pub(crate) fn token_label(n: usize) -> String {
    format!("token#{n}")
}

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{:.1}ms ", self.at.as_secs_f64() * 1000.0)?;
        match &self.stimulus {
            Stimulus::Join { claims, .. } => write!(f, "join {}", claims.principal)?,
            Stimulus::JoinTransfer { transfer, .. } => write!(
                f,
                "join transfer {} from {}",
                transfer.claims.principal, transfer.source_zone
            )?,
            Stimulus::Reconnect {
                token,
                content_version,
                ..
            } => match content_version {
                Some(version) => write!(f, "reconnect {token} version {version}")?,
                None => write!(f, "reconnect {token}")?,
            },
            Stimulus::Disconnect { client, graceful } => {
                write!(f, "disconnect client {} graceful={graceful}", client.0)?
            }
            Stimulus::Input {
                client,
                seq,
                client_tick,
                payload,
            } => write!(
                f,
                "input client {} seq {seq} client_tick {client_tick} payload {payload:02x?}",
                client.0
            )?,
            Stimulus::Content { client, version } => {
                write!(f, "content client {} {version}", client.0)?
            }
            Stimulus::Focus { client, x, y } => write!(f, "focus client {} {x},{y}", client.0)?,
            Stimulus::Kick { principal, .. } => write!(f, "inbox kick {principal}")?,
            Stimulus::Message { type_id, .. } => write!(f, "inbox message {type_id}")?,
            Stimulus::ZoneMessage(envelope) => write!(
                f,
                "zone message {} from {} type {}",
                envelope.id, envelope.from, envelope.type_id
            )?,
            Stimulus::ChildEvent(event) => {
                write!(f, "child event {} from {}", event.kind, event.origin_zone)?
            }
            Stimulus::RequestFreeze(notice) => write!(f, "request freeze {}", notice.reason)?,
            Stimulus::CancelFreeze(notice) => write!(f, "cancel freeze {}", notice.reason)?,
            Stimulus::Resume(notice) => write!(f, "resume {}", notice.reason)?,
            Stimulus::ContentVersion(version) => write!(f, "content version {version}")?,
            Stimulus::ConfigVersion(version) => write!(f, "config version {version}")?,
            Stimulus::Reload(player) => write!(f, "reload player {}", player.0)?,
            Stimulus::ReloadStale => write!(f, "reload stale players")?,
            Stimulus::KickPlayer { player, message } => {
                write!(f, "kick player {}: {message}", player.0)?
            }
            Stimulus::Handoff {
                player,
                target_zone,
                ..
            } => write!(f, "handoff player {} to {target_zone}", player.0)?,
            Stimulus::Restore { state, .. } => write!(f, "restore {} bytes", state.len())?,
            Stimulus::Dispose => write!(f, "dispose")?,
            Stimulus::Advance => write!(f, "advance")?,
        }
        match &self.observed {
            Observed::Nothing => Ok(()),
            Observed::Join(result) => write!(f, " -> {result:?}"),
            Observed::Input(outcome) => write!(f, " -> {outcome:?}"),
            Observed::Advance { settled, hashes } => {
                match (hashes.first(), hashes.last()) {
                    (Some(a), Some(b)) => write!(f, " ticks {}..={}", a.0, b.0)?,
                    _ => write!(f, " no ticks")?,
                }
                write!(f, ", {} settled", settled.len())
            }
        }
    }
}

/// A recorded session.
#[derive(Debug, Clone, PartialEq)]
pub struct Recording {
    /// Zone id (seeds the RNG).
    pub zone_id: String,
    /// Host configuration, as resolved at creation (the boot epoch).
    pub config: HostConfig,
    /// Clock when the host was created.
    pub created_at: Duration,
    /// Wall time (Unix ms) when the host was created.
    pub created_unix_ms: u64,
    /// Fact queue capacity, when a queue was attached.
    pub fact_capacity: Option<usize>,
    /// Stimuli in order.
    pub entries: Vec<Entry>,
}

/// File decode failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingError {
    /// Not a `DGR4` file.
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
    w.u8(match c.stale_policy {
        StalePolicy::Strict => 0,
        StalePolicy::SafeRoom { roam: false } => 1,
        StalePolicy::SafeRoom { roam: true } => 2,
    });
    put_u64(w, c.boot_epoch);
    w.string(&c.config_version);
    w.bool(c.lifecycle_facts);
}

fn put_claims(w: &mut Writer, claims: &TicketClaims) {
    w.string(&claims.principal)
        .string(&claims.zone)
        .string(&claims.nonce);
    put_u64(w, claims.expires_at);
    w.u8(match claims.role {
        Role::Player => 0,
        Role::Observer => 1,
    });
    match &claims.name {
        Some(name) => w.u8(1).string(name),
        None => w.u8(0),
    };
    w.string(&claims.extensions.to_json().to_string());
}

fn get_claims(r: &mut Reader<'_>) -> Result<TicketClaims, RecordingError> {
    let principal = r.string()?;
    let zone = r.string()?;
    let nonce = r.string()?;
    let expires_at = get_u64(r)?;
    let role = match r.u8()? {
        0 => Role::Player,
        _ => Role::Observer,
    };
    let name = match r.u8()? {
        0 => None,
        _ => Some(r.string()?),
    };
    let extensions = serde_json::from_str(&r.string()?)
        .ok()
        .and_then(|value| crate::claims::TicketExtensions::from_json(value).ok())
        .ok_or_else(|| bad("extension claims"))?;
    Ok(TicketClaims {
        principal,
        zone,
        expires_at,
        nonce,
        role,
        name,
        extensions,
        wake: false,
    })
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
        // Update rates only shape Deltas, never the replayed world.
        interest: InterestConfig {
            cell_size: r.f32()?,
            leave_margin: r.f32()?,
            ..InterestConfig::default()
        },
        content_version: r.string()?,
        world_seed: get_u64(r)?,
        rewind: RewindConfig {
            max_rewind: get_dur(r)?,
            max_frames: get_u64(r)? as usize,
        },
        stale_policy: match r.u8()? {
            0 => StalePolicy::Strict,
            1 => StalePolicy::SafeRoom { roam: false },
            _ => StalePolicy::SafeRoom { roam: true },
        },
        boot_epoch: get_u64(r)?,
        config_version: r.string()?,
        lifecycle_facts: r.bool()?,
        record: false,
    })
}

fn put_opt_string(w: &mut Writer, v: Option<&str>) {
    match v {
        Some(v) => w.u8(1).string(v),
        None => w.u8(0),
    };
}

fn get_opt_string(r: &mut Reader<'_>) -> Result<Option<String>, RecordingError> {
    Ok(match r.u8()? {
        0 => None,
        _ => Some(r.string()?),
    })
}

fn put_stimulus(w: &mut Writer, stimulus: &Stimulus) {
    match stimulus {
        Stimulus::Join {
            claims,
            capabilities,
            content_version,
        } => {
            w.u8(0);
            put_claims(w, claims);
            w.varuint(*capabilities).string(content_version);
        }
        Stimulus::JoinTransfer {
            transfer,
            capabilities,
            content_version,
        } => {
            w.u8(1);
            put_claims(w, &transfer.claims);
            w.string(&transfer.source_zone).string(&transfer.issuer);
            put_u64(w, transfer.generation);
            put_u64(w, transfer.departed_at_ms);
            w.bytes(&transfer.carried)
                .varuint(*capabilities)
                .string(content_version);
        }
        Stimulus::Reconnect {
            token,
            capabilities,
            content_version,
        } => {
            w.u8(2).string(token).varuint(*capabilities);
            put_opt_string(w, content_version.as_deref());
        }
        Stimulus::Disconnect { client, graceful } => {
            w.u8(3).varuint(client.0).bool(*graceful);
        }
        Stimulus::Input {
            client,
            seq,
            client_tick,
            payload,
        } => {
            w.u8(4)
                .varuint(client.0)
                .varuint(*seq)
                .varuint(*client_tick)
                .bytes(payload);
        }
        Stimulus::Content { client, version } => {
            w.u8(5).varuint(client.0).string(version);
        }
        Stimulus::Focus { client, x, y } => {
            w.u8(6).varuint(client.0).f32(*x).f32(*y);
        }
        Stimulus::Kick { principal, reason } => {
            w.u8(7).string(principal).string(reason);
        }
        Stimulus::Message { type_id, payload } => {
            w.u8(8).varuint(*type_id).bytes(payload);
        }
        Stimulus::ZoneMessage(envelope) => {
            w.u8(9)
                .string(&envelope.id)
                .string(&envelope.from)
                .string(&envelope.to);
            put_opt_string(w, envelope.principal.as_deref());
            w.varuint(envelope.type_id).bytes(&envelope.payload);
        }
        Stimulus::ChildEvent(event) => {
            w.u8(10)
                .string(&event.origin_zone)
                .string(&event.from_zone)
                .string(&event.kind)
                .bytes(&event.payload);
            put_u64(w, event.origin_tick);
            w.string(&event.content_version).u8(event.levels);
        }
        Stimulus::RequestFreeze(notice) => {
            w.u8(11);
            put_notice(w, notice);
        }
        Stimulus::CancelFreeze(notice) => {
            w.u8(12);
            put_notice(w, notice);
        }
        Stimulus::Resume(notice) => {
            w.u8(13);
            put_notice(w, notice);
        }
        Stimulus::ContentVersion(version) => {
            w.u8(14).string(version);
        }
        Stimulus::ConfigVersion(version) => {
            w.u8(15).string(version);
        }
        Stimulus::Reload(player) => {
            w.u8(16).varuint(player.0);
        }
        Stimulus::ReloadStale => {
            w.u8(17);
        }
        Stimulus::KickPlayer { player, message } => {
            w.u8(18).varuint(player.0).string(message);
        }
        Stimulus::Handoff {
            player,
            target_zone,
            ticket,
        } => {
            w.u8(19)
                .varuint(player.0)
                .string(target_zone)
                .string(ticket);
        }
        Stimulus::Restore {
            state,
            saved_version,
        } => {
            w.u8(20).bytes(state).string(saved_version);
        }
        Stimulus::Dispose => {
            w.u8(21);
        }
        Stimulus::Advance => {
            w.u8(22);
        }
    }
}

fn get_stimulus(r: &mut Reader<'_>) -> Result<Stimulus, RecordingError> {
    Ok(match r.u8()? {
        0 => Stimulus::Join {
            claims: get_claims(r)?,
            capabilities: r.varuint()?,
            content_version: r.string()?,
        },
        1 => {
            let claims = get_claims(r)?;
            let source_zone = r.string()?;
            let issuer = r.string()?;
            let generation = get_u64(r)?;
            let departed_at_ms = get_u64(r)?;
            let carried = r.bytes()?.to_vec();
            Stimulus::JoinTransfer {
                transfer: Transfer {
                    claims,
                    source_zone,
                    issuer,
                    generation,
                    carried,
                    departed_at_ms,
                },
                capabilities: r.varuint()?,
                content_version: r.string()?,
            }
        }
        2 => Stimulus::Reconnect {
            token: r.string()?,
            capabilities: r.varuint()?,
            content_version: get_opt_string(r)?,
        },
        3 => Stimulus::Disconnect {
            client: ClientId(r.varuint()?),
            graceful: r.bool()?,
        },
        4 => Stimulus::Input {
            client: ClientId(r.varuint()?),
            seq: r.varuint()?,
            client_tick: r.varuint()?,
            payload: r.bytes()?.to_vec(),
        },
        5 => Stimulus::Content {
            client: ClientId(r.varuint()?),
            version: r.string()?,
        },
        6 => Stimulus::Focus {
            client: ClientId(r.varuint()?),
            x: r.f32()?,
            y: r.f32()?,
        },
        7 => Stimulus::Kick {
            principal: r.string()?,
            reason: r.string()?,
        },
        8 => Stimulus::Message {
            type_id: r.varuint()?,
            payload: r.bytes()?.to_vec(),
        },
        9 => Stimulus::ZoneMessage(ZoneEnvelope {
            id: r.string()?,
            from: r.string()?,
            to: r.string()?,
            principal: get_opt_string(r)?,
            type_id: r.varuint()?,
            payload: r.bytes()?.to_vec(),
        }),
        10 => Stimulus::ChildEvent(ChildEvent {
            origin_zone: r.string()?,
            from_zone: r.string()?,
            kind: r.string()?,
            payload: r.bytes()?.to_vec(),
            origin_tick: get_u64(r)?,
            content_version: r.string()?,
            levels: r.u8()?,
        }),
        11 => Stimulus::RequestFreeze(get_notice(r)?),
        12 => Stimulus::CancelFreeze(get_notice(r)?),
        13 => Stimulus::Resume(get_notice(r)?),
        14 => Stimulus::ContentVersion(r.string()?),
        15 => Stimulus::ConfigVersion(r.string()?),
        16 => Stimulus::Reload(PlayerId(r.varuint()?)),
        17 => Stimulus::ReloadStale,
        18 => Stimulus::KickPlayer {
            player: PlayerId(r.varuint()?),
            message: r.string()?,
        },
        19 => Stimulus::Handoff {
            player: PlayerId(r.varuint()?),
            target_zone: r.string()?,
            ticket: r.string()?,
        },
        20 => Stimulus::Restore {
            state: r.bytes()?.to_vec(),
            saved_version: r.string()?,
        },
        21 => Stimulus::Dispose,
        22 => Stimulus::Advance,
        _ => return Err(bad("stimulus tag")),
    })
}

fn put_observed(w: &mut Writer, observed: &Observed) {
    match observed {
        Observed::Nothing => {
            w.u8(0);
        }
        Observed::Join(result) => {
            w.u8(1);
            put_join_result(w, result);
        }
        Observed::Input(outcome) => {
            let code = OUTCOMES.iter().position(|o| o == outcome).unwrap_or(0) as u8;
            w.u8(2).u8(code);
        }
        Observed::Advance { settled, hashes } => {
            w.u8(3);
            put_u64(w, settled.len() as u64);
            for (tick, s) in settled {
                put_settlement(w, *tick, s);
            }
            put_u64(w, hashes.len() as u64);
            if let Some((first, _)) = hashes.first() {
                put_u64(w, *first);
            }
            for (_, hash) in hashes {
                w.raw(&hash.to_le_bytes());
            }
        }
    }
}

fn get_observed(r: &mut Reader<'_>) -> Result<Observed, RecordingError> {
    Ok(match r.u8()? {
        0 => Observed::Nothing,
        1 => Observed::Join(get_join_result(r)?),
        2 => Observed::Input(*OUTCOMES.get(r.u8()? as usize).ok_or(bad("input outcome"))?),
        3 => {
            let n = get_u64(r)? as usize;
            let mut settled = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                settled.push(get_settlement(r)?);
            }
            let n = get_u64(r)?;
            let mut hashes = Vec::with_capacity((n as usize).min(1 << 16));
            if n > 0 {
                let first = get_u64(r)?;
                for i in 0..n {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(r.raw(8)?);
                    hashes.push((first + i, u64::from_le_bytes(b)));
                }
            }
            Observed::Advance { settled, hashes }
        }
        _ => return Err(bad("observed tag")),
    })
}

impl Recording {
    /// Compact binary encoding:
    /// `"DGR4" ‖ zone_id ‖ config ‖ created_at ‖ created_unix_ms ‖
    /// fact_capacity? ‖ varuint(entry count) ‖ entries`, each entry
    /// `at ‖ unix_ms ‖ stimulus ‖ observed`. Integers are LEB128 varints,
    /// durations are nanosecond varints, clock readings are delta-coded
    /// against the previous entry (both clocks never go backwards in a
    /// recording: the host clamps them), hashes are 8 raw bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.raw(MAGIC).string(&self.zone_id);
        put_config(&mut w, &self.config);
        put_dur(&mut w, self.created_at);
        put_u64(&mut w, self.created_unix_ms);
        match self.fact_capacity {
            Some(c) => {
                w.u8(1);
                put_u64(&mut w, c as u64);
            }
            None => {
                w.u8(0);
            }
        }
        put_u64(&mut w, self.entries.len() as u64);
        let (mut at, mut unix_ms) = (self.created_at, self.created_unix_ms);
        for entry in &self.entries {
            put_dur(&mut w, entry.at.saturating_sub(at));
            put_u64(&mut w, entry.unix_ms.saturating_sub(unix_ms));
            (at, unix_ms) = (entry.at, entry.unix_ms);
            put_stimulus(&mut w, &entry.stimulus);
            put_observed(&mut w, &entry.observed);
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
        let created_unix_ms = get_u64(&mut r)?;
        let fact_capacity = match r.u8()? {
            0 => None,
            _ => Some(get_u64(&mut r)? as usize),
        };
        let count = get_u64(&mut r)? as usize;
        let mut entries = Vec::with_capacity(count.min(1 << 20));
        let (mut at, mut unix_ms) = (created_at, created_unix_ms);
        for _ in 0..count {
            at += get_dur(&mut r)?;
            unix_ms = unix_ms
                .checked_add(get_u64(&mut r)?)
                .ok_or(bad("wall clock overflow"))?;
            entries.push(Entry {
                at,
                unix_ms,
                stimulus: get_stimulus(&mut r)?,
                observed: get_observed(&mut r)?,
            });
        }
        r.finish()?;
        Ok(Self {
            zone_id,
            config,
            created_at,
            created_unix_ms,
            fact_capacity,
            entries,
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
        self.entries
            .iter()
            .map(|e| match &e.observed {
                Observed::Advance { hashes, .. } => hashes.len(),
                _ => 0,
            })
            .sum()
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
    /// Index into [`Recording::entries`].
    pub entry_index: usize,
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
                "replay diverged at tick {} (entry #{}): state hash {actual:016x} != recorded {expected:016x}",
                self.tick, self.entry_index
            )?,
            DivergenceKind::Ticks { expected, actual } => write!(
                f,
                "replay diverged at tick {} (entry #{}): simulated ticks {actual:?} != recorded {expected:?}",
                self.tick, self.entry_index
            )?,
            DivergenceKind::InputOutcome { expected, actual } => write!(
                f,
                "replay diverged at tick {} (entry #{}): input outcome {actual:?} != recorded {expected:?}",
                self.tick, self.entry_index
            )?,
            DivergenceKind::Join { expected, actual } => write!(
                f,
                "replay diverged at tick {} (entry #{}): join {actual:?} != recorded {expected:?}",
                self.tick, self.entry_index
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
#[derive(Debug, Clone, PartialEq)]
pub struct ReplaySummary {
    /// Entries applied.
    pub entries: usize,
    /// Ticks simulated and verified.
    pub ticks: u64,
    /// Hash after the last tick.
    pub final_hash: u64,
    /// Facts the zone emitted, in order (a recording without a fact queue
    /// emits none).
    pub facts: Vec<Fact>,
}

/// The clock a replay runs on: exactly the recorded readings.
struct ReplayClock(Mutex<(Duration, u64)>);

impl ReplayClock {
    fn set(&self, at: Duration, unix_ms: u64) {
        *self.0.lock().expect("replay clock") = (at, unix_ms);
    }
}

impl Clock for ReplayClock {
    fn now(&self) -> Duration {
        self.0.lock().expect("replay clock").0
    }

    fn unix_ms(&self) -> u64 {
        self.0.lock().expect("replay clock").1
    }
}

/// Replay `recording` through a fresh host on its recorded clock readings
/// and verify every join and input outcome and per-tick state hash.
pub fn replay<Z: Zone>(
    recording: &Recording,
    zone_config: Z::Config,
) -> Result<ReplaySummary, Divergence> {
    replay_with::<Z>(recording, zone_config, |_| {})
}

/// [`replay`] with a callback after each entry (inspection, tooling).
pub fn replay_with<Z: Zone>(
    recording: &Recording,
    zone_config: Z::Config,
    mut inspect: impl FnMut(&ZoneHost<Z>),
) -> Result<ReplaySummary, Divergence> {
    let clock = Arc::new(ReplayClock(Mutex::new((
        recording.created_at,
        recording.created_unix_ms,
    ))));
    let mut issued = 0usize;
    let mut host = ZoneHost::<Z>::new(
        recording.zone_id.clone(),
        recording.config.clone(),
        clock.clone(),
        zone_config,
    )
    .with_token_source(move || {
        issued += 1;
        token_label(issued - 1)
    })
    .with_transfer_issuer(|_: &HandoffRequest| Some("replay-transfer".into()));
    host.set_tick_hashing(true);
    let mut script = None;
    if let Some(capacity) = recording.fact_capacity {
        let (queue, s) = FactQueue::scripted(capacity);
        host = host.with_facts(queue.with_journal());
        script = Some(s);
    }
    let mut last_good_tick = 0u64;
    let mut suspects: Vec<String> = Vec::new();
    let mut last_hash = 0u64;
    let mut ticks = 0u64;
    let mut facts = Vec::new();
    for (index, entry) in recording.entries.iter().enumerate() {
        clock.set(entry.at, entry.unix_ms);
        if let (Some(script), Observed::Advance { settled, .. }) = (&script, &entry.observed) {
            for (tick, s) in settled {
                script.push(*tick, s.clone());
            }
        }
        let outcome = host.apply(entry.stimulus.clone());
        host.drain_outbound();
        facts.extend(host.take_emitted_facts());
        let diverged =
            |tick: u64, kind: DivergenceKind, last_good_tick: u64, suspects: &[String]| {
                let mut suspects = suspects.to_vec();
                suspects.push(entry.to_string());
                Divergence {
                    entry_index: index,
                    tick,
                    kind,
                    last_good_tick,
                    suspects,
                }
            };
        match (&entry.observed, Observed::of(&outcome)) {
            (Observed::Join(expected), Observed::Join(actual)) if *expected != actual => {
                let kind = DivergenceKind::Join {
                    expected: expected.clone(),
                    actual,
                };
                return Err(diverged(host.tick(), kind, last_good_tick, &suspects));
            }
            (Observed::Input(expected), Observed::Input(actual)) if *expected != actual => {
                let kind = DivergenceKind::InputOutcome {
                    expected: *expected,
                    actual,
                };
                return Err(diverged(host.tick(), kind, last_good_tick, &suspects));
            }
            (
                Observed::Advance { hashes, .. },
                Observed::Advance {
                    hashes: actual_hashes,
                    ..
                },
            ) => {
                for ((tick, expected), (_, actual)) in hashes.iter().zip(&actual_hashes) {
                    if expected != actual {
                        let kind = DivergenceKind::StateHash {
                            expected: *expected,
                            actual: *actual,
                        };
                        return Err(diverged(*tick, kind, last_good_tick, &suspects));
                    }
                    last_good_tick = *tick;
                }
                let expected: Vec<u64> = hashes.iter().map(|(t, _)| *t).collect();
                let actual: Vec<u64> = actual_hashes.iter().map(|(t, _)| *t).collect();
                if expected != actual {
                    let kind = DivergenceKind::Ticks { expected, actual };
                    return Err(diverged(host.tick(), kind, last_good_tick, &suspects));
                }
                if let Some((_, h)) = actual_hashes.last() {
                    last_hash = *h;
                    ticks += actual_hashes.len() as u64;
                    suspects.clear();
                }
                inspect(&host);
                continue;
            }
            _ => {}
        }
        suspects.push(entry.to_string());
        inspect(&host);
    }
    Ok(ReplaySummary {
        entries: recording.entries.len(),
        ticks,
        final_hash: last_hash,
        facts,
    })
}

/// Per-tick hashes of a recording (for diffing two recordings).
pub fn tick_hashes(recording: &Recording) -> BTreeMap<u64, u64> {
    recording
        .entries
        .iter()
        .flat_map(|e| match &e.observed {
            Observed::Advance { hashes, .. } => hashes.clone(),
            _ => Vec::new(),
        })
        .collect()
}
