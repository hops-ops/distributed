//! Presence read model projected from the runtime's player lifecycle facts
//! (`HostConfig::lifecycle_facts`, `distributed_game::lifecycle`).
//!
//! The fact sink hands every lifecycle fact to [`Presence::apply`] instead
//! of a Crawler cell. Facts commit in per-principal order within one zone,
//! but across zones (a handoff, a reconnect elsewhere) commit order says
//! nothing: each entry keeps the [`FactStamp`] of the fact that set it and a
//! fact that was *emitted* earlier (`FactStamp::emission_order`) is ignored,
//! so "left pit-1" committed after "joined pit-2" cannot mark a player
//! offline. A retried fact (same key) applies idempotently.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Mutex;

use distributed_game::lifecycle::Lifecycle;
use distributed_game::FactStamp;
use serde::Serialize;

/// Where a player is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceStatus {
    /// Connected to `zone`.
    Online,
    /// Connection lost; `zone` keeps the slot for the reconnect grace.
    Away,
    /// Left `zone` (or its grace expired).
    Offline,
}

/// One player's presence.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceEntry {
    /// Verified principal (the fact's binding, never its payload).
    pub principal: String,
    /// Zone of the latest transition.
    pub zone: String,
    /// Status after it.
    pub status: PresenceStatus,
    /// Host wall time of that transition (Unix ms).
    pub since_ms: u64,
    #[serde(skip)]
    stamp: FactStamp,
}

/// The projection.
#[derive(Debug, Default)]
pub struct Presence {
    entries: Mutex<BTreeMap<String, PresenceEntry>>,
}

impl Presence {
    /// Apply one lifecycle fact of `principal`. False when an entry set by
    /// a later-emitted fact already exists (stale, ignored).
    pub fn apply(&self, principal: &str, stamp: &FactStamp, event: &Lifecycle) -> bool {
        let status = match event {
            Lifecycle::Joined(_) | Lifecycle::Reconnected(_) => PresenceStatus::Online,
            Lifecycle::ConnectionLost(_) => PresenceStatus::Away,
            Lifecycle::Left(_) | Lifecycle::Disconnected(_) => PresenceStatus::Offline,
        };
        let mut entries = self.entries.lock().expect("presence");
        if let Some(current) = entries.get(principal) {
            if current.stamp.emission_order(stamp) != Ordering::Less {
                return false;
            }
        }
        entries.insert(
            principal.to_string(),
            PresenceEntry {
                principal: principal.to_string(),
                zone: event.zone().to_string(),
                status,
                since_ms: stamp.emitted_at_ms,
                stamp: stamp.clone(),
            },
        );
        true
    }

    /// One player.
    pub fn get(&self, principal: &str) -> Option<PresenceEntry> {
        self.entries
            .lock()
            .expect("presence")
            .get(principal)
            .cloned()
    }

    /// Everyone seen, by principal.
    pub fn snapshot(&self) -> Vec<PresenceEntry> {
        self.entries
            .lock()
            .expect("presence")
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use distributed_game::lifecycle::{JoinVia, LeftReason, PlayerJoined, PlayerLeft};

    fn stamp(zone: &str, activation: u64, tick: u64, at: u64) -> FactStamp {
        FactStamp {
            zone_id: zone.into(),
            tick,
            emitted_at_ms: at,
            activation,
            seq: tick,
            ..FactStamp::default()
        }
    }

    #[test]
    fn a_handoff_leave_committed_late_does_not_mark_the_player_offline() {
        let presence = Presence::default();
        let joined_b = Lifecycle::Joined(PlayerJoined {
            zone: "pit-2".into(),
            via: JoinVia::Handoff,
            from_zone: Some("pit-1".into()),
        });
        let left_a = Lifecycle::Left(PlayerLeft {
            zone: "pit-1".into(),
            reason: LeftReason::Handoff,
        });
        // pit-2 commits first; pit-1's departure (emitted earlier) after.
        assert!(presence.apply("ana", &stamp("pit-2", 4, 2, 1_001), &joined_b));
        assert!(!presence.apply("ana", &stamp("pit-1", 3, 90, 1_000), &left_a));
        let ana = presence.get("ana").unwrap();
        assert_eq!(
            (ana.zone.as_str(), ana.status),
            ("pit-2", PresenceStatus::Online)
        );
        // Replaying the same fact is a no-op.
        assert!(!presence.apply("ana", &stamp("pit-2", 4, 2, 1_001), &joined_b));
    }

    #[test]
    fn a_drop_in_the_join_tick_and_millisecond_still_marks_the_player_away() {
        use distributed_game::lifecycle::{ConnectionLost, LostCause};
        let presence = Presence::default();
        let joined = Lifecycle::Joined(PlayerJoined {
            zone: "pit-1".into(),
            via: JoinVia::Ticket,
            from_zone: None,
        });
        let lost = Lifecycle::ConnectionLost(ConnectionLost {
            zone: "pit-1".into(),
            grace_ms: 15_000,
            cause: LostCause::Dropped,
        });
        // Same zone, activation, tick and wall time: only the host's
        // emission sequence orders them.
        let at = |seq| FactStamp {
            seq,
            ..stamp("pit-1", 3, 0, 1_000)
        };
        assert!(presence.apply("bo", &at(1), &joined));
        assert!(presence.apply("bo", &at(2), &lost));
        assert_eq!(presence.get("bo").unwrap().status, PresenceStatus::Away);
        assert!(!presence.apply("bo", &at(1), &joined), "stale join ignored");
    }
}
