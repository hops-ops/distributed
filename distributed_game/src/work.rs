//! Work counters: how much work a zone did, as plain integers.
//!
//! Timings on a loaded machine move with everything else running on it;
//! counts do not. Two runs of the same build with a switch flipped (fact
//! batches, update rates, pushed world views) compare exactly on these:
//! records encoded per patch, updates sent or held back, cell requests per
//! fact, world reads per room-second.
//!
//! The celld zone host keeps one [`WorkCounters`] per object (they survive
//! the zone's sleeps, not an eviction) and reports it as `work` in `GET
//! /zones/{zone}/info`; a watched cell reports its side
//! ([`WatcherStats`](crate::world::watch::WatcherStats)) at `GET
//! /{kind}/{shard}/__watchers`. `mobpit-load` reads both around each step.

use serde::{Deserialize, Serialize};

use crate::host::AdvanceReport;

/// Monotonic work counts of one zone (camelCase JSON).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WorkCounters {
    /// Patches (Delta rounds).
    pub patches: u64,
    /// Entity records encoded, shared by every client
    /// ([`EncodedWorld::encodes`](crate::replication::EncodedWorld::encodes)).
    pub encodes: u64,
    /// Delta frames built (one per client per patch).
    pub frames: u64,
    /// Delta bytes, all clients.
    pub delta_bytes: u64,
    /// Entity creates sent.
    pub creates: u64,
    /// Entity updates sent.
    pub updates: u64,
    /// Entity updates held back by the update rates (a held entity counts
    /// once per client per patch).
    pub held: u64,
    /// Creates and updates deferred by the byte budget.
    pub deferred: u64,
    /// Fact commands sent to cells (each entry of a batch counts).
    pub fact_commands: u64,
    /// Requests that carried them (a `__batch` counts once).
    pub fact_requests: u64,
    /// Of those, `__batch` requests.
    pub fact_batches: u64,
    /// Outbox delete calls for settled facts (one per batch when batched).
    pub outbox_deletes: u64,
    /// Reads of watched world cells (wake, reconcile, or polls when the
    /// zone takes no pushes).
    pub world_reads: u64,
    /// World view pushes received.
    pub world_pushes: u64,
}

impl WorkCounters {
    /// Add one [`ZoneHost::advance`](crate::ZoneHost::advance).
    pub fn record_advance(&mut self, report: &AdvanceReport) {
        self.patches += u64::from(report.patched);
        self.encodes += report.encodes as u64;
        for frame in &report.frames {
            self.frames += 1;
            self.delta_bytes += frame.bytes as u64;
            self.creates += frame.stats.creates as u64;
            self.updates += frame.stats.updates as u64;
            self.held += frame.stats.held as u64;
            self.deferred += frame.stats.deferred as u64;
        }
    }

    /// The work done since `earlier` (`None` when a count went backwards:
    /// a different object instance).
    pub fn since(&self, earlier: &Self) -> Option<Self> {
        let d = |now: u64, then: u64| now.checked_sub(then);
        Some(Self {
            patches: d(self.patches, earlier.patches)?,
            encodes: d(self.encodes, earlier.encodes)?,
            frames: d(self.frames, earlier.frames)?,
            delta_bytes: d(self.delta_bytes, earlier.delta_bytes)?,
            creates: d(self.creates, earlier.creates)?,
            updates: d(self.updates, earlier.updates)?,
            held: d(self.held, earlier.held)?,
            deferred: d(self.deferred, earlier.deferred)?,
            fact_commands: d(self.fact_commands, earlier.fact_commands)?,
            fact_requests: d(self.fact_requests, earlier.fact_requests)?,
            fact_batches: d(self.fact_batches, earlier.fact_batches)?,
            outbox_deletes: d(self.outbox_deletes, earlier.outbox_deletes)?,
            world_reads: d(self.world_reads, earlier.world_reads)?,
            world_pushes: d(self.world_pushes, earlier.world_pushes)?,
        })
    }

    /// Element-wise sum (several zones).
    pub fn add(&mut self, other: &Self) {
        self.patches += other.patches;
        self.encodes += other.encodes;
        self.frames += other.frames;
        self.delta_bytes += other.delta_bytes;
        self.creates += other.creates;
        self.updates += other.updates;
        self.held += other.held;
        self.deferred += other.deferred;
        self.fact_commands += other.fact_commands;
        self.fact_requests += other.fact_requests;
        self.fact_batches += other.fact_batches;
        self.outbox_deletes += other.outbox_deletes;
        self.world_reads += other.world_reads;
        self.world_pushes += other.world_pushes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EntitySchema, EntityState, FieldType, FieldValue, KindDef, ManualClock, Role, TicketClaims,
        WorldView, Zone, ZoneHost,
    };
    use std::sync::Arc;
    use std::time::Duration;

    /// Eight dots that all move every tick.
    struct Dots(u32);

    impl Zone for Dots {
        type Config = ();
        type Input = u8;
        type Timer = ();
        type Message = distributed_game::NoMessages;

        fn schema() -> EntitySchema {
            EntitySchema::new().kind(KindDef::new(1, "dot").field("v", FieldType::U16))
        }

        fn on_create(_ctx: &mut crate::Ctx<'_, Self>, _: ()) -> Self {
            Dots(0)
        }

        fn decode_input(&self, _p: crate::PlayerId, b: &[u8]) -> Option<u8> {
            b.first().copied()
        }

        fn on_tick(&mut self, _ctx: &mut crate::Ctx<'_, Self>) {
            self.0 += 1;
        }

        fn replicate(&self, out: &mut WorldView) {
            for i in 0..8 {
                out.push(EntityState::new(
                    i,
                    1,
                    i as f32,
                    0.0,
                    vec![FieldValue::U(self.0 + i)],
                ));
            }
        }
    }

    fn claims(principal: &str) -> TicketClaims {
        TicketClaims {
            principal: principal.into(),
            zone: "z".into(),
            expires_at: u64::MAX,
            nonce: principal.into(),
            role: Role::Player,
            name: None,
            extensions: Default::default(),
            wake: false,
        }
    }

    #[test]
    fn counts_shared_encodes_and_per_client_records() {
        let clock = ManualClock::new();
        let mut host: ZoneHost<Dots> =
            ZoneHost::new("z", Default::default(), Arc::new(clock.clone()), ());
        for p in ["a", "b", "c"] {
            host.join(&claims(p), 0, "").unwrap();
        }
        let mut work = WorkCounters::default();
        for _ in 0..10 {
            clock.advance(Duration::from_millis(50));
            work.record_advance(&host.advance());
        }
        assert!(work.patches >= 5, "{work:?}");
        assert_eq!(work.frames, work.patches * 3);
        assert_eq!(work.creates, 8 * 3, "each client gets each dot once");
        // Every dot changes every patch and every client is current: one
        // payload and one shared update per dot and patch (the first patch
        // has no updates), however many clients.
        assert_eq!(work.encodes, 8 + (work.patches - 1) * 16, "{work:?}");
        assert_eq!(work.updates + work.held, (work.patches - 1) * 8 * 3);
        assert!(work.delta_bytes > 0);
        let later = WorkCounters {
            world_reads: 2,
            ..work
        };
        assert_eq!(later.since(&work).map(|d| d.world_reads), Some(2));
        assert_eq!(work.since(&later), None, "went backwards");
    }
}
