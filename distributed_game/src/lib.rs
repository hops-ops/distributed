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

#[cfg(feature = "bots")]
pub mod bots;
#[cfg(feature = "cell")]
pub mod cell;
pub mod clock;
pub mod facts;
pub mod handoff;
pub mod host;
pub mod input;
pub mod interest;
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "net")]
pub mod net;
pub mod protocol;
pub mod replay;
pub mod replication;
pub mod rewind;
pub mod rng;
pub mod schema;
pub mod stamp;
pub mod timers;
pub mod timestep;
pub mod world;
pub mod zone;

pub use clock::{Clock, ManualClock, SystemClock};
#[cfg(feature = "runtime")]
pub use facts::spawn_fact_worker;
pub use facts::{
    fact_channel, EmitError, Fact, FactError, FactFeed, FactOutcome, FactQueue, FactReceipt,
    FactSettlement, FactSink, MemoryFactSink, RetryPolicy, SettlementScript,
};
pub use handoff::{HandoffError, HandoffRequest, Transfer};
pub use host::{AdvanceReport, Ctx, FrameStats, HostConfig, JoinError, Joined, Outbound, ZoneHost};
pub use input::{ConsumePolicy, InputBuffer, PushOutcome, SequencedInput};
pub use interest::{InterestConfig, View};
pub use protocol::{ClientMessage, ClientMirror, Role, ServerMessage, PROTOCOL_VERSION};
pub use replay::{replay, Recorder, Recording};
pub use replication::{EntityState, WorldView};
pub use rewind::{Historic, HitQuery, Rewind, RewindConfig, RewindFrame, ShotTiming};
pub use rng::{Fnv64, ZoneRng};
pub use schema::{EntitySchema, FieldType, FieldValue, KindDef};
pub use stamp::{stamp_metadata, FactStamp};
pub use timers::TimerId;
pub use timestep::FixedTimestep;
pub use world::{BarrierMode, ChildEvent, FreezeState, StalePolicy};
pub use zone::{
    Admission, ClientId, JoinRequest, LeaveReason, ObserverId, ObserverInfo, PlayerId, PlayerInfo,
    TicketClaims, Tier, Viewer, Zone,
};

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct ReadmeDoctests;
