//! Data-driven content of **The Freeze**: one pack per content version.
//!
//! Version `v{n}` uses pack `(n - 1) % 3`, so the admin can keep rolling
//! out "new" versions. The server derives its rules (hazard, slime speed,
//! gem value) from the pack of the zone's content version; clients fetch
//! the same pack (`GET /dev/freeze/content/{version}`, the demo's stand-in
//! for a content CDN) and derive colors, props and labels from it. A client
//! pinned to an old version keeps rendering its old pack.
//!
//! Everything here is defined once, in Rust. A [`Pack`] is a
//! `#[derive(GameRecord)]`: the CDN serves its payload-codec encoding and
//! the generated client decodes it (`decodePack`, from
//! `GameSurface::record::<Pack>()`). The version-independent layout
//! (zones, portals, pools, crystal spots) is [`freeze_world`], the
//! generated `FREEZE_WORLD` constant (`GameSurface::constant`). Packs are
//! *served* rather than compiled into the client because the point of the
//! demo is a client receiving content it was not built with.

use distributed_game::codec::{Wire, WireType};
use distributed_game::protocol::{DecodeError, Reader, Writer};
use distributed_game::GameRecord;

/// What standing in a pool does (on the wire: `"lava"` / `"ice"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hazard {
    /// Burns: the player is sent back to a spawn point.
    Lava,
    /// Safe to cross.
    Ice,
}

impl Hazard {
    /// Wire name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Lava => "lava",
            Self::Ice => "ice",
        }
    }
}

impl Wire for Hazard {
    fn wire_type() -> WireType {
        WireType::ShortStr
    }
    fn encode(&self, w: &mut Writer) {
        w.short_string(self.name());
    }
    fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        match r.short_string()?.as_str() {
            "lava" => Ok(Self::Lava),
            "ice" => Ok(Self::Ice),
            _ => Err(DecodeError::Invalid("unknown hazard")),
        }
    }
}

/// One content pack (served encoded; the client's `Pack` is generated).
#[derive(Debug, Clone, PartialEq, GameRecord)]
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
    /// Index into [`FreezeWorld::crystals`] (mirrored per pack).
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

    /// What the content CDN serves: the payload-codec encoding.
    pub fn to_bytes(&self) -> Vec<u8> {
        distributed_game::codec::to_bytes(self)
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

/// A point of a zone (m).
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct Spot {
    /// x.
    pub x: f32,
    /// y.
    pub y: f32,
}

/// A portal to another zone.
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct Portal {
    /// x.
    pub x: f32,
    /// y.
    pub y: f32,
    /// Target zone id.
    pub to: String,
    /// Target zone title.
    pub title: String,
}

/// A pool (lava or ice, by pack).
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct Pool {
    /// x.
    pub x: f32,
    /// y.
    pub y: f32,
    /// Radius.
    pub r: f32,
}

/// The layout of one zone.
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct ZoneLayout {
    /// Zone id.
    pub id: String,
    /// Display title.
    pub title: String,
    /// The safe-room gate (the hub).
    pub gate: bool,
    /// Portals.
    pub portals: Vec<Portal>,
    /// Pools.
    pub pools: Vec<Pool>,
    /// Where a ritual holds the zone.
    pub ritual: Spot,
}

/// The version-independent world (generated `FREEZE_WORLD`).
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct FreezeWorld {
    /// Town hub.
    pub hub: ZoneLayout,
    /// Whisper Meadow.
    pub meadow: ZoneLayout,
    /// Ember Fields.
    pub ember: ZoneLayout,
    /// Crystal spots per layout id ([`Pack::crystal_layout`]).
    pub crystals: Vec<Vec<Spot>>,
}

/// The `FREEZE_WORLD` constant.
pub fn freeze_world() -> FreezeWorld {
    let zone = |kind: ZoneKind, id: &str| ZoneLayout {
        id: id.into(),
        title: kind.title().into(),
        gate: kind == ZoneKind::Hub,
        portals: kind
            .portals()
            .iter()
            .map(|&(x, y, to)| Portal {
                x,
                y,
                to: to.into(),
                title: ZoneKind::of(to).map_or("", ZoneKind::title).into(),
            })
            .collect(),
        pools: kind
            .pools()
            .iter()
            .map(|&(x, y, r)| Pool { x, y, r })
            .collect(),
        ritual: Spot {
            x: kind.ritual().0,
            y: kind.ritual().1,
        },
    };
    FreezeWorld {
        hub: zone(ZoneKind::Hub, HUB),
        meadow: zone(ZoneKind::Meadow, MEADOW),
        ember: zone(ZoneKind::Ember, EMBER),
        crystals: (0..PACKS as u8)
            .map(|layout| {
                crystals(layout)
                    .into_iter()
                    .map(|(x, y)| Spot { x, y })
                    .collect()
            })
            .collect(),
    }
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
        let served: Pack =
            distributed_game::codec::from_bytes(&pack("v2").unwrap().to_bytes()).unwrap();
        assert_eq!(served, pack("v2").unwrap());
        let world = freeze_world();
        assert_eq!(world.ember.portals[0].to, HUB);
        assert_eq!(world.ember.portals[0].title, ZoneKind::Hub.title());
        assert_eq!(world.crystals.len(), PACKS as usize);
        assert!(pack("v2")
            .unwrap()
            .resume_notes()
            .starts_with("Frostfall Patch (v2)\n"));
    }

    #[test]
    fn layout_is_clear_of_pillars() {
        use game_sim::arena::PILLARS;
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
