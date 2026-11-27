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

mod codec;
mod run;

pub use run::{replay, replay_with, tick_hashes, Divergence, DivergenceKind, ReplaySummary};

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
