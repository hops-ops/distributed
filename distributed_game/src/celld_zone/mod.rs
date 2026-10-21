//! Zones as celld Durable Objects (feature `celld-zone`): host any [`Zone`]
//! as one object per zone id. celld decides where a zone runs, forwards
//! every connection to it, moves idle zones, evicts sleeping ones and
//! recovers them after a node crash; game code and game APIs never name a
//! server.
//!
//! ```text
//! client ──WebSocket /rooms/{zone}──▶ any celld node ──forward──▶ the zone's object
//!                                                         │ ZoneHost<Z> (this activation)
//!         Hello(ticket | transfer ticket | reconnect token) ┤ deadline tick loop while occupied
//!                                                         │ snapshot every few s + when empty
//!                     facts (outbox in object storage) ───┼──▶ cell objects (CommandFactSink
//!                                                         │     over CelldObjectHost, exactly once)
//!     zone_activity.record (active / dormant / hibernated)┘──▶ zone-activity/{zone} cell
//!                                                               └ outbox ▶ Queue ▶ zone_status read model
//! ```
//!
//! What the framework does per object ([`ZoneObject`], wasm32):
//! - **Join.** A WebSocket upgrade is accepted as a hibernatable socket;
//!   its attachment holds the client id and the activation id. Hello is
//!   verified with the shared HS256 [`TicketKey`](crate::ticket::TicketKey)
//!   (env `GAME_TICKET_SECRET`): a join ticket, a transfer ticket from a
//!   neighbouring zone's handoff (single use, also across activations) or a
//!   reconnect token inside the grace window.
//! - **Ticks.** A self-correcting `setTimeout` loop over absolute deadlines
//!   ([`TickSchedule`]; `setInterval` drifts) drives
//!   [`ZoneHost::advance`](crate::ZoneHost::advance) while anyone is
//!   connected, and stops when the zone is empty.
//! - **State.** The zone's [`Zone::hibernate`] snapshot is written to object
//!   storage every `ZONE_PERSIST_MS` while occupied and when the last
//!   player leaves (`dormant`); after `ZONE_HIBERNATE_AFTER_MS` empty (an
//!   object alarm, so it survives eviction) the zone is dropped from memory
//!   (`hibernated`). The next join wakes it and restores the snapshot.
//! - **Restarts.** A socket that outlives its activation (eviction, a move,
//!   a crash) is closed with [`CLOSE_RESTARTED`] (1012); the client rejoins
//!   with a fresh ticket.
//! - **Facts.** Zone facts go to cells of the same worker object to object
//!   ([`CelldObjectHost`] under [`CommandFactSink`](crate::command_sink::CommandFactSink),
//!   the same routes and command ids as over HTTP). Each fact is written to
//!   the object's outbox before it is sent and deleted when settled; the
//!   next activation or the outbox alarm resends what is left, and the
//!   cells' command ledger turns a resend into a replay: exactly once.
//! - **Status.** The zone reports its lifecycle as facts into its
//!   [`zone_activity`](crate::world::zone_activity) cell, projected into the
//!   [`zone_status`](crate::world::zone_status) read model (minimaps).
//!
//! What celld does: placement (first touch, overflow by
//! `CELLD_MAX_RESIDENT_CELLS`), forwarding from any node, idle eviction,
//! rebalancing of idle objects, graceful drain and crash recovery (fenced
//! by its own epochs; one instance per object name).
//!
//! Limits: a live zone never moves (only idle ones are rebalanced), so
//! spreading load depends on first touch and node caps; objects of one
//! class share V8 isolates (a slow zone delays its neighbours); a crash
//! loses at most the snapshot interval of zone state (facts are kept by
//! the outbox) and takes the node lease TTL (~10 s) to recover.
//!
//! **Observers are passive watchers** on celld: an observer connection
//! (a ticket with role `observer`, e.g. a page watching the rooms next to
//! its own) never counts as occupancy — a zone with only observers stops
//! ticking (they keep its last state), goes dormant and hibernates (their
//! sockets are closed) — and an observer Hello never wakes a sleeping zone:
//! it is refused with [`error::ASLEEP`](crate::protocol::error::ASLEEP).
//! Only a player wakes a zone. A watcher joining a dormant zone gets its
//! current state at once ([`ZoneHost::patch_now`](crate::ZoneHost::patch_now)).
//!
//! Dev only: with `ZONE_DEV_SETTINGS=1` every zone object applies the
//! network simulation ([`NetSim`](crate::netsim::NetSim): latency, jitter,
//! loss per socket and direction) stored in the worker's
//! [`ZoneDevSettings`] object (`zone-dev/netsim`), read on wake and every
//! second while it ticks; a dev route writes it (one setting for every
//! zone, like the native server's `set_net_sim`).
//!
//! Debug only: celld does not tell application code which node runs an
//! object. With `CELLD_ZONE_NODES` set (operator listeners) the object asks
//! each node whether it holds it; `CELLD_ZONE_NODE` names a fixed node.
//! Both are off by default; nothing routes by them.

pub mod plan;

#[cfg(target_arch = "wasm32")]
mod cells;
#[cfg(target_arch = "wasm32")]
mod object;

pub use plan::{
    cell_binding, ObjectOptions, RoomState, StoredFact, TickSchedule, CLOSE_RESTARTED,
    DEV_SETTINGS_VAR, NETSIM_SHARD, ZONE_DEV_KIND, ZONE_HEADER,
};

#[cfg(target_arch = "wasm32")]
pub use cells::{
    authenticate, cell_alarm, error_response, fact_sessions, json_status, serve_cell, Sessions,
    ZoneActivityCell, ZoneDevSettings,
};
#[cfg(target_arch = "wasm32")]
pub use object::{
    forward_to_zone, CelldObjectHost, CelldZone, LocalFuture, ObjectEnv, ObjectFactSink, ZoneObject,
};

#[cfg(doc)]
use crate::Zone;
