//! Tick stamping: provenance of committed facts.
//!
//! Every [`Fact`](crate::Fact) carries a [`FactStamp`] filled in by the host
//! when the zone emits it: zone id, activation, tick, host wall time,
//! content version and config version. The cell adapter forwards the stamp
//! as trusted session variables; command handlers copy it into
//! `EventRecord.metadata` with [`stamp_metadata`] (or `stamp_event_record`
//! with the `cell` feature) so every durable event records exactly which
//! simulation tick produced it.
//!
//! **Ordering.** Commit order is not emission order: facts travel through
//! per-principal lanes and retries, and two zones commit independently. A
//! handler that must order facts (presence: "left A" vs "joined B" after a
//! handoff) compares stamps with [`FactStamp::emission_order`]. Within one
//! zone activation the emission sequence `seq` decides (the host numbers
//! every fact it stamps, so two facts of the same tick and millisecond
//! still have a strict order); across activations of one zone the
//! activation (it grows across restarts); across zones the host wall time
//! `emitted_at_ms`. A handoff target never stamps the arriving player's
//! facts earlier than the source stamped its departure: the transfer ticket
//! carries the departure time and the target's stamps start after it, even
//! when the two hosts' clocks disagree.

use std::cmp::Ordering;
use std::collections::HashMap;

/// Metadata key for the zone id.
pub const META_ZONE: &str = "game.zone";
/// Metadata key for the zone tick (decimal).
pub const META_TICK: &str = "game.tick";
/// Metadata key for the content version.
pub const META_CONTENT_VERSION: &str = "game.content_version";
/// Metadata key for the host wall time at emission (Unix ms, decimal).
pub const META_EMITTED_AT_MS: &str = "game.emitted_at_ms";
/// Metadata key for the zone activation (boot epoch, decimal).
pub const META_ACTIVATION: &str = "game.activation";
/// Metadata key for the config version.
pub const META_CONFIG_VERSION: &str = "game.config_version";
/// Metadata key for the per-activation emission sequence (decimal).
pub const META_SEQ: &str = "game.seq";

/// Session-variable prefix used to carry a stamp through a trusted Session.
pub const SESSION_PREFIX: &str = "x-game-";

/// Where and when a fact was produced.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FactStamp {
    /// Zone id.
    pub zone_id: String,
    /// Zone tick during which the fact was emitted.
    pub tick: u64,
    /// Content version the zone was running.
    pub content_version: String,
    /// Host wall time at emission, Unix milliseconds
    /// ([`Clock::unix_ms`](crate::Clock::unix_ms), never earlier than a
    /// handoff source's departure stamp).
    pub emitted_at_ms: u64,
    /// Zone activation ([`HostConfig::boot_epoch`](crate::HostConfig::boot_epoch):
    /// the ownership fencing token under the `net` server); grows across
    /// restarts and hibernation.
    pub activation: u64,
    /// Remote config version the zone was running
    /// ([`HostConfig::config_version`](crate::HostConfig::config_version)).
    pub config_version: String,
    /// Emission sequence within the activation: the host numbers every fact
    /// it stamps 1, 2, 3, ... so facts of one tick (a join and a dropped
    /// socket in the same tick and millisecond) keep a strict order. A stamp
    /// taken for anything else ([`Ctx::stamp`](crate::Ctx::stamp)) carries
    /// the sequence of the last fact stamped before it.
    pub seq: u64,
}

impl FactStamp {
    /// Metadata entries for this stamp.
    pub fn entries(&self) -> [(&'static str, String); 7] {
        [
            (META_ZONE, self.zone_id.clone()),
            (META_TICK, self.tick.to_string()),
            (META_CONTENT_VERSION, self.content_version.clone()),
            (META_EMITTED_AT_MS, self.emitted_at_ms.to_string()),
            (META_ACTIVATION, self.activation.to_string()),
            (META_CONFIG_VERSION, self.config_version.clone()),
            (META_SEQ, self.seq.to_string()),
        ]
    }

    /// Session variables (`x-game-zone`, `x-game-tick`,
    /// `x-game-content-version`, `x-game-emitted-at-ms`,
    /// `x-game-activation`, `x-game-config-version`, `x-game-seq`).
    pub fn session_vars(&self) -> [(String, String); 7] {
        [
            (format!("{SESSION_PREFIX}zone"), self.zone_id.clone()),
            (format!("{SESSION_PREFIX}tick"), self.tick.to_string()),
            (
                format!("{SESSION_PREFIX}content-version"),
                self.content_version.clone(),
            ),
            (
                format!("{SESSION_PREFIX}emitted-at-ms"),
                self.emitted_at_ms.to_string(),
            ),
            (
                format!("{SESSION_PREFIX}activation"),
                self.activation.to_string(),
            ),
            (
                format!("{SESSION_PREFIX}config-version"),
                self.config_version.clone(),
            ),
            (format!("{SESSION_PREFIX}seq"), self.seq.to_string()),
        ]
    }

    /// Read a stamp back from session variables (None if any is
    /// absent/malformed).
    pub fn from_session_vars(vars: &HashMap<String, String>) -> Option<Self> {
        let get = |name: &str| vars.get(&format!("{SESSION_PREFIX}{name}"));
        Some(Self {
            zone_id: get("zone")?.clone(),
            tick: get("tick")?.parse().ok()?,
            content_version: get("content-version")?.clone(),
            emitted_at_ms: get("emitted-at-ms")?.parse().ok()?,
            activation: get("activation")?.parse().ok()?,
            config_version: get("config-version")?.clone(),
            seq: get("seq")?.parse().ok()?,
        })
    }

    /// Read a stamp back from event metadata written by [`stamp_metadata`]
    /// (None if any entry is absent/malformed).
    pub fn from_metadata(metadata: &HashMap<String, String>) -> Option<Self> {
        Some(Self {
            zone_id: metadata.get(META_ZONE)?.clone(),
            tick: metadata.get(META_TICK)?.parse().ok()?,
            content_version: metadata.get(META_CONTENT_VERSION)?.clone(),
            emitted_at_ms: metadata.get(META_EMITTED_AT_MS)?.parse().ok()?,
            activation: metadata.get(META_ACTIVATION)?.parse().ok()?,
            config_version: metadata.get(META_CONFIG_VERSION)?.clone(),
            seq: metadata.get(META_SEQ)?.parse().ok()?,
        })
    }

    /// Emission order of two stamps: same zone and activation by emission
    /// sequence (then tick and wall time); same zone across activations by
    /// activation; different
    /// zones by wall time (then zone id, activation and tick, so the order
    /// is total and deterministic). Commit order says nothing about this.
    pub fn emission_order(&self, other: &Self) -> Ordering {
        if self.zone_id == other.zone_id {
            self.activation
                .cmp(&other.activation)
                .then(self.seq.cmp(&other.seq))
                .then(self.tick.cmp(&other.tick))
                .then(self.emitted_at_ms.cmp(&other.emitted_at_ms))
        } else {
            self.emitted_at_ms
                .cmp(&other.emitted_at_ms)
                .then_with(|| self.zone_id.cmp(&other.zone_id))
                .then(self.activation.cmp(&other.activation))
                .then(self.seq.cmp(&other.seq))
                .then(self.tick.cmp(&other.tick))
        }
    }

    /// True when this stamp was emitted after `other`
    /// ([`emission_order`](Self::emission_order)).
    pub fn emitted_after(&self, other: &Self) -> bool {
        self.emission_order(other) == Ordering::Greater
    }
}

/// Insert the stamp into an event metadata map.
pub fn stamp_metadata(metadata: &mut HashMap<String, String>, stamp: &FactStamp) {
    for (key, value) in stamp.entries() {
        metadata.insert(key.to_string(), value);
    }
}

/// Stamp a `distributed` event record's metadata.
#[cfg(feature = "cell")]
pub fn stamp_event_record(record: &mut distributed::EventRecord, stamp: &FactStamp) {
    stamp_metadata(&mut record.metadata, stamp);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamp(zone: &str, activation: u64, tick: u64, at: u64) -> FactStamp {
        FactStamp {
            zone_id: zone.into(),
            tick,
            content_version: "c7".into(),
            emitted_at_ms: at,
            activation,
            config_version: "combat@12".into(),
            // Ticks only grow within an activation, so neither does seq.
            seq: tick,
        }
    }

    #[test]
    fn stamp_round_trips_through_session_vars_and_metadata() {
        let stamp = stamp("z1", 9, 42, 1_700_000_000_123);
        let vars: HashMap<_, _> = stamp.session_vars().into_iter().collect();
        assert_eq!(FactStamp::from_session_vars(&vars), Some(stamp.clone()));
        let mut meta = HashMap::new();
        stamp_metadata(&mut meta, &stamp);
        assert_eq!(meta[META_TICK], "42");
        assert_eq!(meta[META_ZONE], "z1");
        assert_eq!(meta[META_EMITTED_AT_MS], "1700000000123");
        assert_eq!(meta[META_ACTIVATION], "9");
        assert_eq!(meta[META_CONFIG_VERSION], "combat@12");
        assert_eq!(meta[META_SEQ], "42");
        assert_eq!(FactStamp::from_metadata(&meta), Some(stamp));
    }

    #[test]
    fn missing_or_malformed_fields_are_rejected() {
        let stamp = stamp("z1", 9, 42, 5);
        let mut vars: HashMap<_, _> = stamp.session_vars().into_iter().collect();
        vars.remove("x-game-activation");
        assert_eq!(FactStamp::from_session_vars(&vars), None);
        let mut vars: HashMap<_, _> = stamp.session_vars().into_iter().collect();
        vars.insert("x-game-emitted-at-ms".into(), "soon".into());
        assert_eq!(FactStamp::from_session_vars(&vars), None);
        let mut meta = HashMap::new();
        stamp_metadata(&mut meta, &stamp);
        meta.remove(META_CONFIG_VERSION);
        assert_eq!(FactStamp::from_metadata(&meta), None);
    }

    #[test]
    fn emission_order_by_tick_activation_then_wall_time() {
        // One activation: the tick wins over a wall clock read out of order.
        assert!(stamp("a", 1, 11, 100).emitted_after(&stamp("a", 1, 10, 200)));
        // A later activation of the same zone is later, ticks restart.
        assert!(stamp("a", 2, 1, 50).emitted_after(&stamp("a", 1, 900, 60)));
        // Different zones: wall time.
        assert!(stamp("b", 1, 1, 201).emitted_after(&stamp("a", 7, 99, 200)));
        assert!(!stamp("a", 7, 99, 200).emitted_after(&stamp("b", 1, 1, 201)));
        // Ties are broken deterministically.
        let (x, y) = (stamp("a", 1, 1, 5), stamp("b", 1, 1, 5));
        assert_eq!(x.emission_order(&y), y.emission_order(&x).reverse());
        // Same tick and millisecond: the emission sequence decides.
        let (first, second) = (
            FactStamp {
                seq: 7,
                ..stamp("a", 1, 3, 9)
            },
            FactStamp {
                seq: 8,
                ..stamp("a", 1, 3, 9)
            },
        );
        assert!(second.emitted_after(&first));
        assert!(!first.emitted_after(&second));
    }
}
