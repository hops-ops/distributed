//! Tick stamping: provenance of committed facts.
//!
//! Every [`Fact`](crate::Fact) carries a [`FactStamp`] (zone id, tick,
//! content version) filled in by the host when the zone emits it. The cell
//! adapter forwards the stamp as trusted session variables; command handlers
//! copy it into `EventRecord.metadata` with [`stamp_metadata`] (or
//! `stamp_event_record` with the `cell` feature) so every durable event
//! records exactly which simulation tick produced it.

use std::collections::HashMap;

/// Metadata key for the zone id.
pub const META_ZONE: &str = "game.zone";
/// Metadata key for the zone tick (decimal).
pub const META_TICK: &str = "game.tick";
/// Metadata key for the content version.
pub const META_CONTENT_VERSION: &str = "game.content_version";

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
}

impl FactStamp {
    /// Metadata entries for this stamp.
    pub fn entries(&self) -> [(&'static str, String); 3] {
        [
            (META_ZONE, self.zone_id.clone()),
            (META_TICK, self.tick.to_string()),
            (META_CONTENT_VERSION, self.content_version.clone()),
        ]
    }

    /// Session variables (`x-game-zone`, `x-game-tick`, `x-game-content-version`).
    pub fn session_vars(&self) -> [(String, String); 3] {
        [
            (format!("{SESSION_PREFIX}zone"), self.zone_id.clone()),
            (format!("{SESSION_PREFIX}tick"), self.tick.to_string()),
            (
                format!("{SESSION_PREFIX}content-version"),
                self.content_version.clone(),
            ),
        ]
    }

    /// Read a stamp back from session variables (None if absent/malformed).
    pub fn from_session_vars(vars: &HashMap<String, String>) -> Option<Self> {
        Some(Self {
            zone_id: vars.get(&format!("{SESSION_PREFIX}zone"))?.clone(),
            tick: vars.get(&format!("{SESSION_PREFIX}tick"))?.parse().ok()?,
            content_version: vars
                .get(&format!("{SESSION_PREFIX}content-version"))?
                .clone(),
        })
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

    #[test]
    fn stamp_round_trips_through_session_vars_and_metadata() {
        let stamp = FactStamp {
            zone_id: "z1".into(),
            tick: 42,
            content_version: "c7".into(),
        };
        let vars: HashMap<_, _> = stamp.session_vars().into_iter().collect();
        assert_eq!(FactStamp::from_session_vars(&vars), Some(stamp.clone()));
        let mut meta = HashMap::new();
        stamp_metadata(&mut meta, &stamp);
        assert_eq!(meta[META_TICK], "42");
        assert_eq!(meta[META_ZONE], "z1");
    }
}
