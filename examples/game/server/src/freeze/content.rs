//! Data-driven content of **The Freeze**: one pack per content version.
//!
//! Version `v{n}` uses pack `(n - 1) % 3`, so the admin can keep rolling
//! out "new" versions. The server derives its rules (hazard, slime speed,
//! gem value) from the pack of the zone's content version; clients fetch
//! the same pack as JSON (`GET /dev/freeze/content/{version}`, the demo's
//! stand-in for a content CDN) and derive colors, props and labels from
//! it. A client pinned to an old version keeps rendering its old pack.
//!
//! The zone layout (portals, pools, gem spots) is version independent and
//! shipped inside every pack so the client needs nothing else.

use serde::Serialize;
use serde_json::{json, Value};

/// What standing in a pool does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Hazard {
    /// Burns: the player is sent back to a spawn point.
    Lava,
    /// Safe to cross.
    Ice,
}

/// One content pack.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Pack {
    /// `v{n}`.
    pub version: String,
    /// Display name of the update.
    pub name: String,
    /// "Here's what changed" bullet points (relative to the previous pack).
    pub notes: Vec<String>,
    /// Pool behaviour.
    pub hazard: Hazard,
    /// Slime wander speed (m/s).
    pub slime_speed: f32,
    /// Slime color (hex).
    pub slime_color: String,
    /// Slime scale.
    pub slime_scale: f32,
    /// Crystal prop color (hex).
    pub crystal_color: String,
    /// Crystal props `(x, y)` per field zone (mirrored per pack).
    pub crystal_layout: u8,
    /// Value of a collected gem.
    pub gem_value: u32,
}

/// Number of distinct packs.
pub const PACKS: u32 = 3;

/// Version number of `version` (`"v7"` → 7).
pub fn version_number(version: &str) -> Option<u32> {
    version
        .strip_prefix('v')?
        .parse()
        .ok()
        .filter(|n: &u32| *n >= 1)
}

/// `"v{n}"`.
pub fn version_name(n: u32) -> String {
    format!("v{n}")
}

/// The pack of `version` (`None` for a malformed version).
pub fn pack(version: &str) -> Option<Pack> {
    let n = version_number(version)?;
    let (name, notes, hazard, speed, color, scale, crystal, layout, gem) = match (n - 1) % PACKS {
        0 => (
            "Ember Season",
            vec![
                "The thaw is over: the pools are molten lava again (they burn).",
                "Slimes are back to a calm 2.0 m/s and green.",
                "Crystals glow orange; gems are worth 1.",
            ],
            Hazard::Lava,
            2.0,
            "#7ed957",
            1.0,
            "#ff9d3c",
            0,
            1,
        ),
        1 => (
            "Frostfall Patch",
            vec![
                "Lava is now ice: the pools are safe to cross.",
                "Slimes are faster (2.0 → 3.5 m/s), bigger and blue.",
                "Crystals moved and turned cyan; gems are worth 2.",
            ],
            Hazard::Ice,
            3.5,
            "#5ab8ff",
            1.35,
            "#7ff3ff",
            1,
            2,
        ),
        _ => (
            "Bloom Update",
            vec![
                "Spring thaw: the ice melted back into lava (it burns again).",
                "Slimes are slow (1.2 m/s), tiny and pink.",
                "Crystals bloomed magenta; gems are worth 3.",
            ],
            Hazard::Lava,
            1.2,
            "#ff7ad9",
            0.7,
            "#d77bff",
            2,
            3,
        ),
    };
    Some(Pack {
        version: version_name(n),
        name: name.into(),
        notes: notes.into_iter().map(String::from).collect(),
        hazard,
        slime_speed: speed,
        slime_color: color.into(),
        slime_scale: scale,
        crystal_color: crystal.into(),
        crystal_layout: layout,
        gem_value: gem,
    })
}

impl Pack {
    /// Notes for the barrier's `Resume{notes}`: the name, then one line per
    /// change.
    pub fn resume_notes(&self) -> String {
        let mut out = format!("{} ({})", self.name, self.version);
        for note in &self.notes {
            out.push('\n');
            out.push_str(note);
        }
        out
    }

    /// Pack plus the world layout, as served to clients.
    pub fn to_json(&self) -> Value {
        let mut v = serde_json::to_value(self).expect("pack serializes");
        v["zones"] = layout_json();
        v
    }
}

/// Kind of zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZoneKind {
    /// Town hub: the safe room (gate).
    Hub,
    /// Field zone on the primary host.
    Meadow,
    /// Field zone on the second host.
    Ember,
}

/// Zone ids of the world.
pub const HUB: &str = "freeze-hub";
/// Meadow zone id.
pub const MEADOW: &str = "freeze-meadow";
/// Ember Fields zone id.
pub const EMBER: &str = "freeze-ember";
/// Every zone (activated at startup so the barrier reaches them all).
pub const ZONES: [&str; 3] = [HUB, MEADOW, EMBER];

impl ZoneKind {
    /// Kind of a zone id.
    pub fn of(zone_id: &str) -> Option<Self> {
        match zone_id {
            HUB => Some(Self::Hub),
            MEADOW => Some(Self::Meadow),
            EMBER => Some(Self::Ember),
            _ => None,
        }
    }

    /// Display title.
    pub fn title(self) -> &'static str {
        match self {
            Self::Hub => "Town Hub (safe room)",
            Self::Meadow => "Whisper Meadow",
            Self::Ember => "Ember Fields",
        }
    }

    /// Portals `(x, y, target zone)`.
    pub fn portals(self) -> &'static [(f32, f32, &'static str)] {
        match self {
            Self::Hub => &[(-16.5, 5.0, MEADOW), (16.5, 5.0, EMBER)],
            Self::Meadow => &[(16.5, 5.0, HUB)],
            Self::Ember => &[(-16.5, 5.0, HUB)],
        }
    }

    /// Pools `(x, y, radius)`.
    pub fn pools(self) -> &'static [(f32, f32, f32)] {
        match self {
            Self::Hub => &[],
            Self::Meadow => &[(-6.0, 6.0, 2.2), (6.0, -5.0, 2.5)],
            Self::Ember => &[(-5.0, -5.0, 2.5), (6.0, 5.0, 3.0), (-4.0, 12.0, 2.0)],
        }
    }

    /// Gem spots.
    pub fn gem_spots(self) -> &'static [(f32, f32)] {
        match self {
            Self::Hub => &[],
            Self::Meadow | Self::Ember => &[
                (-12.0, -12.0),
                (12.0, -12.0),
                (12.0, 12.0),
                (-12.0, 12.0),
                (-3.0, -16.0),
                (3.0, 16.0),
            ],
        }
    }

    /// Wandering slimes.
    pub fn slimes(self) -> u32 {
        match self {
            Self::Hub => 0,
            Self::Meadow => 6,
            Self::Ember => 8,
        }
    }

    /// Where a "ritual" can hold the zone (the stuck-zone toggle).
    pub fn ritual(self) -> (f32, f32) {
        (0.0, -10.0)
    }
}

/// Fresh-join spawn points (clear of pools and pillars).
pub const SPAWNS: [(f32, f32); 4] = [(0.0, -6.0), (0.0, 6.0), (-6.0, 0.0), (6.0, 0.0)];

/// Crystal props for a layout id (decor, no collision).
pub fn crystals(layout: u8) -> Vec<(f32, f32)> {
    let base = [
        (-17.0, -17.0),
        (-11.0, -17.5),
        (17.0, -16.0),
        (17.5, 11.0),
        (-17.5, 15.0),
        (-8.0, 17.0),
        (10.0, 17.5),
    ];
    base.iter()
        .map(|&(x, y)| match layout {
            0 => (x, y),
            1 => (-x, y),
            _ => (y, x),
        })
        .collect()
}

fn layout_json() -> Value {
    let zone = |kind: ZoneKind| {
        json!({
            "title": kind.title(),
            "gate": kind == ZoneKind::Hub,
            "portals": kind.portals().iter().map(|(x, y, to)| json!({"x": x, "y": y, "to": to, "title": ZoneKind::of(to).map(ZoneKind::title)})).collect::<Vec<_>>(),
            "pools": kind.pools().iter().map(|(x, y, r)| json!({"x": x, "y": y, "r": r})).collect::<Vec<_>>(),
            "ritual": { "x": kind.ritual().0, "y": kind.ritual().1 },
        })
    };
    json!({
        HUB: zone(ZoneKind::Hub),
        MEADOW: zone(ZoneKind::Meadow),
        EMBER: zone(ZoneKind::Ember),
        "crystals": [crystals(0), crystals(1), crystals(2)],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_cycle_through_packs() {
        assert_eq!(version_number("v12"), Some(12));
        assert_eq!(version_number("v0"), None);
        assert_eq!(version_number("x1"), None);
        assert_eq!(pack("v1").unwrap().hazard, Hazard::Lava);
        assert_eq!(pack("v2").unwrap().hazard, Hazard::Ice);
        assert_eq!(pack("v4").unwrap().name, pack("v1").unwrap().name);
        assert_eq!(pack("v4").unwrap().version, "v4");
        let json = pack("v2").unwrap().to_json();
        assert_eq!(json["hazard"], "ice");
        assert_eq!(json["zones"][EMBER]["portals"][0]["to"], HUB);
        assert!(pack("v2")
            .unwrap()
            .resume_notes()
            .starts_with("Frostfall Patch (v2)\n"));
    }

    #[test]
    fn layout_is_clear_of_pillars() {
        use crate::arena::movement::PILLARS;
        for kind in [ZoneKind::Hub, ZoneKind::Meadow, ZoneKind::Ember] {
            let points = kind
                .portals()
                .iter()
                .map(|p| (p.0, p.1))
                .chain(kind.gem_spots().iter().copied())
                .chain(SPAWNS);
            for (x, y) in points {
                for (px, py, pr) in PILLARS {
                    assert!(((x - px).powi(2) + (y - py).powi(2)).sqrt() > pr + 1.0);
                }
                for (qx, qy, qr) in kind.pools() {
                    assert!(((x - qx).powi(2) + (y - qy).powi(2)).sqrt() > qr + 1.0);
                }
            }
        }
    }
}
