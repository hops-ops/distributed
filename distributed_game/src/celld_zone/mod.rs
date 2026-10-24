//! Zones as celld Durable Objects (feature `celld-zone`): host any [`Zone`]
//! as one object per zone id. celld decides where a zone runs, forwards
//! every connection to it, moves idle zones, evicts sleeping ones and
//! recovers them after a node crash; game code and game APIs never name a
//! server.
//!
//! ```text
//! client ──WebSocket /zones/{zone}──▶ any celld node ──forward──▶ the zone's object
//!                                                         │ ZoneHost<Z> (this activation)
//!         Hello(ticket | transfer ticket | reconnect token) ┤ deadline tick loop while occupied
//!                                                         │ snapshot every min + on leave/empty
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
//!   connected, and stops `ZONE_DORMANT_AFTER_MS` (10 s) after the zone
//!   empties.
//! - **State.** The zone's [`Zone::hibernate`] snapshot is written to object
//!   storage every `ZONE_PERSIST_MS` (60 s) while occupied, on the tick a
//!   player leaves (leave, handoff, grace expired) and when the loop stops
//!   (`dormant`); after `ZONE_HIBERNATE_AFTER_MS` empty (an
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
//! - **The world around.** A zone *watches* the world cells it cares
//!   about ([`CelldZone::watch`], [`crate::world::watch`]): each is read
//!   on wake, then pushed by its cell ([`CellWatchers`]) after every
//!   change, and re-read every ~45 s (renewing the zone's in-memory watch
//!   lease, repairing a lost push). Each view decodes into a plain struct
//!   that becomes a zone message. The object takes pushes at its class
//!   binding (`celld_zone_object!(PitRoom, …)` → `PIT_ROOM`). Custom async
//!   reads still go through [`CelldZone::refresh`] /
//!   [`CelldZone::refresh_every`].
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
//! Only a player — or a *near* watcher — wakes a zone. A watcher joining
//! a dormant zone gets its current state at once
//! ([`ZoneHost::patch_now`](crate::ZoneHost::patch_now)).
//!
//! **Near watchers** (proximity wake): an observer whose ticket grants
//! `wake` ([`TicketClaims::wake`](crate::TicketClaims::wake)) and that says
//! it is near (Hello `near`, later
//! [`ClientMessage::Near`](crate::protocol::ClientMessage::Near), no
//! reconnect) counts as occupancy like a player: it wakes a sleeping zone
//! and keeps it ticking. When it says it is no longer near, or closes, the
//! zone goes dormant and hibernates as usual. Without the grant the
//! watcher stays passive and is told
//! [`error::WAKE_DENIED`](crate::protocol::error::WAKE_DENIED) (still
//! connected; a fresh ticket may carry the grant). The ticket issuer bounds who may wake what (e.g. only rooms
//! next to the principal's own); the zone checks only the signed grant.
//!
//! **Live content updates.** With `ZONE_CONTENT_ROLLOUT=<world>` every
//! zone object follows the world's
//! [`content_rollout`](crate::world::content_rollout) cell
//! ([`ContentRolloutCell`], binding `CONTENT_ROLLOUT`): it watches the cell
//! (each phase is pushed) and runs
//! [`zone_step`](crate::world::content_rollout::zone_step) — freeze with
//! the change notes, then switch to the new version and resume (a hard
//! rollout reloads stale clients at once). A zone that slept through a
//! rollout restores its snapshot's version and catches up on wake; a Hello
//! whose content version differs from the zone's waits for that read
//! before admission, so a doorway into a sleeping zone admits on the live
//! version. Stale clients are handled by the host's
//! [`StalePolicy`](crate::world::StalePolicy).
//!
//! Dev only: with `ZONE_DEV_SETTINGS=1` every zone object applies the
//! network simulation ([`NetSim`](crate::netsim::NetSim): latency, jitter,
//! loss per socket and direction) stored in the worker's
//! [`ZoneDevSettings`] object (`zone-dev/netsim`), watched like the content
//! rollout (each write is pushed); a dev route writes it (one setting for
//! every zone, like the native server's `set_net_sim`).
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
    CONTENT_ROLLOUT_VAR, DEV_SETTINGS_VAR, NETSIM_SHARD, ZONE_DEV_KIND, ZONE_HEADER,
};

#[cfg(target_arch = "wasm32")]
pub use cells::{
    authenticate, cell_alarm, error_response, fact_sessions, json_status, serve_cell, CellWatchers,
    ContentRolloutCell, Sessions, ZoneActivityCell, ZoneDevSettings,
};
#[cfg(target_arch = "wasm32")]
pub use object::{
    forward_to_zone, CelldObjectHost, CelldZone, LocalFuture, ObjectEnv, ObjectFactSink, ZoneObject,
};

#[cfg(doc)]
use crate::Zone;
