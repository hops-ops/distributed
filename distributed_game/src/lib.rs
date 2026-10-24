//! Authoritative multiplayer zone runtime for `distributed`.
//!
//! - [`Zone`] (implemented by the game) + [`ZoneHost`] (runtime): lifecycle
//!   hooks, capacity, per-zone timers, fixed timestep with catch-up cap,
//!   activity tiers (Hot / Idle / Dormant), sequenced input buffers with acks.
//! - Determinism: injectable [`Clock`] and per-zone seeded [`ZoneRng`].
//! - Durable outcomes: zones emit [`Fact`]s during a tick; a [`FactSink`]
//!   commits them asynchronously and settlements come back on later ticks.
//!   With the `cell` feature, [`cell::CellFactSink`] dispatches them into a
//!   `distributed` `AggregateCell` idempotently with a server-built Session.
//! - Replication: an [`EntitySchema`] shipped in Welcome, per-client deltas
//!   against last-sent state, spatial interest and a byte budget with
//!   priority accumulation. Wire format: [`protocol`] / `PROTOCOL.md`.
//! - Transport (`net` feature): axum WebSocket server hosting many zones
//!   with HMAC-signed ticket admission ([`net`]).
//! - celld (`celld-zone` feature, wasm32): each zone is a celld Durable
//!   Object; celld places, forwards, hibernates and recovers it
//!   ([`celld_zone`]).
//!
//! - Authoring derives (re-exported from `distributed_game_macros`):
//!   `#[derive(Replicated)]` views generate a kind's schema and field
//!   encoding from one struct ([`schema!`], [`WorldView::add`]);
//!   `#[derive(GameInput)]` generates the input codec and sanitize;
//!   `#[derive(GameEvent)]` / `#[derive(GameRecord)]` give events stable ids
//!   and typed binary payloads ([`Ctx::send`], [`Ctx::broadcast`]);
//!   `#[derive(FactCommand)]` binds a cell command input to its command name
//!   ([`Ctx::emit`]). The derives also describe the zone's
//!   [`GameSurface`](surface::GameSurface), from which `distributed
//!   game-client` generates typed TypeScript.
//! - Client prediction ([`prediction`]): `#[predict]` declares a zone's
//!   predicted step once in Rust; `distributed game-wasm` compiles it to
//!   WebAssembly (binary ABI, no JSON) and the generated client predicts
//!   with it.
//! - Shared client logic and data ([`pure`], [`GameSurface::constant`](surface::GameSurface::constant)):
//!   `#[game_pure]` compiles any other pure function (procedural content,
//!   …) into the same module with a typed generated wrapper; constants and
//!   standalone records are emitted as generated TypeScript data / codecs.
//!
//! Physics is out of scope: games bring their own and run it in `on_tick`.
//!
//! - World structure ([`world`]): zone directory and consistent-hash ring,
//!   fenced ownership, hibernation snapshots, zone hierarchy with tier-up
//!   events, handoff with carried state ([`handoff`]) and the content
//!   barrier / safe-room policy.
//!
//! # Extension seams (later phases)
//! Metrics hook [`AdvanceReport`] and [`replication::DeltaStats`].

#![warn(missing_docs)]

// Lets the authoring derives' `::distributed_game::…` paths resolve inside
// this crate too.
extern crate self as distributed_game;

pub mod authoring;
#[cfg(feature = "bots")]
pub mod bots;
#[cfg(feature = "cell")]
pub mod cell;
#[cfg(feature = "celld")]
pub mod celld;
#[cfg(feature = "celld-zone")]
pub mod celld_zone;
pub mod claims;
pub mod clock;
pub mod codec;
#[cfg(feature = "cell")]
pub mod command_sink;
pub mod entity;
pub mod facts;
pub mod handoff;
pub mod host;
pub mod inbox;
pub mod input;
pub mod interest;
pub mod lifecycle;
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "net")]
pub mod net;
pub mod netsim;
pub mod prediction;
pub mod protocol;
pub mod pure;
pub mod replay;
pub mod replication;
pub mod rewind;
pub mod rng;
pub mod schema;
pub mod stamp;
pub mod surface;
/// Signed admission tickets (also `net::ticket`); usable without the axum
/// server, e.g. by a celld zone host on wasm32.
#[cfg(feature = "ticket")]
#[path = "net/ticket.rs"]
pub mod ticket;
pub mod timers;
pub mod timestep;
pub mod world;
pub mod zone;

pub use authoring::{GameEvent, GameInput, Pushed, Replicated};
pub use claims::{ExtensionError, ExtensionSchema, TicketExtension, TicketExtensions};
pub use clock::{Clock, ManualClock, SystemClock};
pub use codec::{Wire, WireType};
/// Authoring derives (see the crate docs and README).
pub use distributed_game_macros::{
    game_pure, predict, FactCommand, GameEvent, GameInput, GameRecord, Replicated, TicketExtension,
};
pub use entity::{Entities, Entity, EntityCollection, EntityId, Project};
#[cfg(feature = "runtime")]
pub use facts::spawn_fact_worker;
pub use facts::{
    fact_channel, fact_key, BoxFuture, EmitError, Fact, FactBatchSink, FactCommand, FactError,
    FactFeed, FactOutcome, FactQueue, FactReceipt, FactSettlement, FactSink, MemoryFactSink,
    RetryPolicy, SettlementScript,
};
pub use handoff::{HandoffError, HandoffRequest, Transfer};
pub use host::{AdvanceReport, Ctx, FrameStats, HostConfig, JoinError, Joined, Outbound, ZoneHost};
pub use inbox::{Delivered, NoMessages, ZoneInbound};
pub use input::{ConsumePolicy, InputBuffer, InputCredits, PushOutcome, SequencedInput};
pub use interest::{InterestConfig, UpdateRates, View};
pub use prediction::{PredictContext, PredictStep};
pub use protocol::{ClientMessage, ClientMirror, Origin, Role, ServerMessage, PROTOCOL_VERSION};
pub use pure::GamePure;
pub use replay::{replay, Recorder, Recording};
pub use replication::{EntityState, WorldView};
pub use rewind::{Historic, HitQuery, Rewind, RewindConfig, RewindFrame, ShotTiming};
pub use rng::{Fnv64, ZoneRng};
pub use schema::{Correction, EntitySchema, FieldType, FieldValue, KindDef};
pub use stamp::{stamp_metadata, FactStamp};
pub use surface::GameSurface;
pub use timers::TimerId;
pub use timestep::FixedTimestep;
pub use world::{BarrierMode, ChildEvent, FreezeState, StalePolicy};
pub use zone::{
    Admission, ClientId, JoinRequest, LeaveReason, ObserverId, ObserverInfo, PlayerId, PlayerInfo,
    TicketClaims, Tier, Viewer, Zone,
};

/// Paths used by derive-generated code. Not public API.
#[doc(hidden)]
pub mod __private {
    pub use crate::authoring::{decode_whole, records_of};
    pub use crate::codec::{
        special, ButtonBits, EntityField, InputField, LongString, QuantizedF32, Wire, WireType,
    };
    #[cfg(target_arch = "wasm32")]
    pub use crate::prediction::wasm_export;
    pub use crate::protocol::wire::{DecodeError, Reader, Writer};
    #[cfg(target_arch = "wasm32")]
    pub use crate::pure::wasm_export as pure_wasm_export;
    pub use crate::schema::{Correction, FieldType, FieldValue, KindDef};
    pub use crate::surface::{
        EventPayload, EventSurface, InputSurface, PayloadField, RecordSurface,
    };
    pub use serde;
    pub use std;
}

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
