//! **Shooting Gallery** — lag compensation by rewinding hit volumes.
//!
//! Targets race along rails and circles downrange; players stand at fixed
//! stations on the firing line and fire a hitscan ray from their eye. A
//! client renders targets ~one-way latency + two patch intervals in the past
//! (interpolation), so what it aimed at is not where the target is on the
//! server when the shot arrives. With **compensation on** the zone resolves
//! the shot with `ctx.rewind().hit_test_shot` at the shooter's view time
//! (input `client_tick` minus the interpolation delay carried in the
//! payload), clamped to [`MAX_REWIND_MS`]; with it **off** the shot is tested
//! against the latest state.
//!
//! Entities:
//!
//! | kind | ids | fields |
//! |---|---|---|
//! | 1 `shooter` | 1000 + player | x y name shots hits |
//! | 2 `target` | 1..=[`TARGETS`] | x y z r flash |
//!
//! Inputs are shots only ([`ShotInput`], 11 bytes). The shooter gets one
//! [`EVENT_SHOT`] (JSON) per shot with the verdict and the rewind used.

use std::collections::BTreeMap;
use std::time::Duration;

use distributed_game::net::{GameServer, ServerConfig};
use distributed_game::rewind::{Historic, HitQuery, RewindFrame, ShotTiming, Vec3};
use distributed_game::{
    ConsumePolicy, Ctx, EntitySchema, EntityState, FieldType, FieldValue, KindDef, LeaveReason,
    PlayerId, View, WorldView, Zone,
};
use serde_json::json;

use crate::common::{send_json_event, Demo, Guests};

/// Shooter kind.
pub const KIND_SHOOTER: u16 = 1;
/// Target kind.
pub const KIND_TARGET: u16 = 2;
/// Event: `{"entity", "eye": [x, y, z], "maxRewindMs", "range"}` (JSON).
pub const EVENT_HELLO: u32 = 1;
/// Event: shot verdict (JSON, see [`ShotVerdict`]).
pub const EVENT_SHOT: u32 = 2;
/// Floor half extent (m).
pub const HALF: f32 = 20.0;
/// Firing line (m, game y).
pub const FIRING_LINE: f32 = -14.0;
/// Eye height (m).
pub const EYE: f32 = 1.6;
/// Station spacing along the firing line (m).
pub const STATION_SPACING: f32 = 3.0;
/// Stations on the firing line.
pub const STATIONS: u32 = 5;
/// Furthest a shot is rewound (anti-cheat clamp).
pub const MAX_REWIND_MS: u64 = 600;
/// Hitscan range (m).
pub const RANGE: f32 = 80.0;
/// Number of targets.
pub const TARGETS: u32 = 6;
/// Input flag: this input is a shot.
pub const FLAG_FIRE: u8 = 1;
/// Input flag: evaluate without lag compensation.
pub const FLAG_NO_COMP: u8 = 2;

const POS: FieldType = FieldType::quant_centered(HALF, 18);

/// How a target moves.
#[derive(Debug, Clone, Copy)]
enum Path {
    /// Ping-pong along x at `y` between `x0..x1` at `speed` m/s.
    Rail {
        y: f32,
        x0: f32,
        x1: f32,
        speed: f32,
    },
    /// Circle around `(cx, cy)` with radius `r` at `speed` m/s.
    Circle {
        cx: f32,
        cy: f32,
        r: f32,
        speed: f32,
    },
}

/// `(path, height, radius)` of every target.
const COURSE: [(Path, f32, f32); TARGETS as usize] = [
    (
        Path::Rail {
            y: 2.0,
            x0: -12.0,
            x1: 12.0,
            speed: 3.0,
        },
        1.4,
        0.7,
    ),
    (
        Path::Rail {
            y: 6.0,
            x0: -14.0,
            x1: 14.0,
            speed: 7.0,
        },
        1.8,
        0.6,
    ),
    (
        Path::Circle {
            cx: -7.0,
            cy: 11.0,
            r: 4.0,
            speed: 8.0,
        },
        2.2,
        0.6,
    ),
    (
        Path::Circle {
            cx: 7.0,
            cy: 11.0,
            r: 3.0,
            speed: 6.0,
        },
        1.2,
        0.6,
    ),
    (
        Path::Rail {
            y: 16.0,
            x0: -16.0,
            x1: 16.0,
            speed: 12.0,
        },
        2.6,
        0.6,
    ),
    (
        Path::Rail {
            y: -2.0,
            x0: -10.0,
            x1: 10.0,
            speed: 10.0,
        },
        1.0,
        0.5,
    ),
];

/// Target position at simulation time `t` (seconds).
pub fn target_position(index: usize, t: f64) -> Vec3 {
    let (path, z, _) = COURSE[index];
    match path {
        Path::Rail { y, x0, x1, speed } => {
            let len = f64::from(x1 - x0);
            let d = (t * f64::from(speed)).rem_euclid(2.0 * len);
            let along = if d <= len { d } else { 2.0 * len - d };
            [x0 + along as f32, y, z]
        }
        Path::Circle { cx, cy, r, speed } => {
            let a = (t * f64::from(speed) / f64::from(r)).rem_euclid(std::f64::consts::TAU);
            [cx + r * a.cos() as f32, cy + r * a.sin() as f32, z]
        }
    }
}

/// Radius of target `index` (0-based).
pub fn target_radius(index: usize) -> f32 {
    COURSE[index].2
}

/// Speed of target `index` (m/s).
pub fn target_speed(index: usize) -> f32 {
    match COURSE[index].0 {
        Path::Rail { speed, .. } | Path::Circle { speed, .. } => speed,
    }
}

/// One shot: `[flags, shot u16, delay_ms u16, dx i16, dy i16, dz i16]`
/// (little endian; the direction is a unit vector × 32767).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShotInput {
    /// [`FLAG_FIRE`] | [`FLAG_NO_COMP`].
    pub flags: u8,
    /// Client shot id (echoed in the verdict).
    pub shot: u16,
    /// Interpolation delay behind `client_tick` the shooter was rendering (ms).
    pub delay_ms: u16,
    /// Unit aim direction (game x, y, z-up).
    pub dir: Vec3,
}

impl ShotInput {
    /// Wire payload.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![self.flags];
        out.extend_from_slice(&self.shot.to_le_bytes());
        out.extend_from_slice(&self.delay_ms.to_le_bytes());
        for v in self.dir {
            out.extend_from_slice(&((v.clamp(-1.0, 1.0) * 32767.0).round() as i16).to_le_bytes());
        }
        out
    }

    /// Parse a payload; `None` for a wrong length or a zero direction.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 11 {
            return None;
        }
        let u16_at = |i: usize| u16::from_le_bytes([bytes[i], bytes[i + 1]]);
        let axis = |i: usize| f32::from(u16_at(i) as i16) / 32767.0;
        let dir = [axis(5), axis(7), axis(9)];
        let len = (dir[0] * dir[0] + dir[1] * dir[1] + dir[2] * dir[2]).sqrt();
        (len > 0.5).then(|| Self {
            flags: bytes[0] & (FLAG_FIRE | FLAG_NO_COMP),
            shot: u16_at(1),
            delay_ms: u16_at(3),
            dir: [dir[0] / len, dir[1] / len, dir[2] / len],
        })
    }
}

/// Per-shooter counters (server truth).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShooterStats {
    /// Shots resolved.
    pub shots: u32,
    /// Shots that hit.
    pub hits: u32,
    /// Shots whose requested rewind exceeded the clamp.
    pub clamped: u32,
}

/// The verdict sent to the shooter.
#[derive(Debug, Clone, PartialEq)]
pub struct ShotVerdict {
    /// Client shot id.
    pub shot: u16,
    /// Target hit.
    pub hit: Option<u32>,
    /// Lag compensation used.
    pub compensated: bool,
    /// Rewind applied (0 without compensation).
    pub age: Duration,
    /// Rewind the client asked for.
    pub requested_age: Duration,
    /// Limited by [`MAX_REWIND_MS`] / the stored history.
    pub clamped: bool,
}

struct Shooter {
    entity: u32,
    name: String,
    station: u32,
    stats: ShooterStats,
}

impl Shooter {
    fn eye(&self) -> Vec3 {
        let (x, y) = station_position(self.station);
        [x, y, EYE]
    }
}

/// Station `i` position on the firing line.
pub fn station_position(station: u32) -> (f32, f32) {
    let offset = station as f32 - (STATIONS - 1) as f32 / 2.0;
    (offset * STATION_SPACING, FIRING_LINE)
}

/// Zone construction parameters.
#[derive(Clone, Default)]
pub struct GalleryConfig {
    /// Display names registered by the dev ticket endpoint.
    pub guests: Guests,
}

/// The Shooting Gallery zone.
pub struct Gallery {
    guests: Guests,
    shooters: BTreeMap<PlayerId, Shooter>,
    targets: Vec<Vec3>,
    flash: Vec<u8>,
}

impl Gallery {
    /// Server-side counters of a player.
    pub fn stats(&self, player: PlayerId) -> Option<ShooterStats> {
        self.shooters.get(&player).map(|s| s.stats)
    }

    /// Current target positions.
    pub fn targets(&self) -> &[Vec3] {
        &self.targets
    }

    fn free_station(&self) -> u32 {
        (0..STATIONS)
            .find(|i| self.shooters.values().all(|s| s.station != *i))
            .unwrap_or(self.shooters.len() as u32 % STATIONS)
    }

    /// Resolve one shot of `player` (compensated or not).
    pub fn resolve_shot(
        &self,
        ctx: &Ctx<'_, Self>,
        player: PlayerId,
        client_tick: u32,
        shot: &ShotInput,
    ) -> Option<ShotVerdict> {
        let shooter = self.shooters.get(&player)?;
        let query = HitQuery::ray(shooter.eye(), shot.dir, RANGE);
        let rewind = ctx.rewind();
        let latest = rewind.latest()?.time();
        let compensated = shot.flags & FLAG_NO_COMP == 0;
        let timing = ShotTiming {
            client_tick,
            interp_delay: Duration::from_millis(shot.delay_ms.into()),
        };
        let resolved = rewind.resolve(timing)?;
        let (time, age) = if compensated {
            (resolved.time, resolved.age)
        } else {
            (latest, Duration::ZERO)
        };
        let hit = rewind
            .hit_test(time, &query, |id| (1..=TARGETS).contains(&id))
            .map(|h| h.id);
        Some(ShotVerdict {
            shot: shot.shot,
            hit,
            compensated,
            age,
            requested_age: resolved.requested_age,
            clamped: compensated && resolved.clamped,
        })
    }
}

impl Zone for Gallery {
    type Config = GalleryConfig;
    type Input = ShotInput;
    type Timer = ();

    fn schema() -> EntitySchema {
        EntitySchema::new()
            .kind(
                KindDef::new(KIND_SHOOTER, "shooter")
                    .field("x", POS)
                    .field("y", POS)
                    .field_with_priority("name", FieldType::ShortStr, 0.2)
                    .field("shots", FieldType::U32)
                    .field("hits", FieldType::U32)
                    .priority(0.5),
            )
            .kind(
                KindDef::new(KIND_TARGET, "target")
                    .field("x", POS)
                    .field("y", POS)
                    .field(
                        "z",
                        FieldType::QuantF32 {
                            min: 0.0,
                            max: 4.0,
                            bits: 12,
                        },
                    )
                    .field("r", FieldType::F32)
                    .field("flash", FieldType::U8)
                    .priority(2.0),
            )
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, config: GalleryConfig) -> Self {
        let t = ctx.sim_time().as_secs_f64();
        Self {
            guests: config.guests,
            shooters: BTreeMap::new(),
            targets: (0..TARGETS as usize)
                .map(|i| target_position(i, t))
                .collect(),
            flash: vec![0; TARGETS as usize],
        }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let principal = ctx
            .player(player)
            .map(|p| p.principal.clone())
            .unwrap_or_default();
        let name = self.guests.name(&principal).unwrap_or(principal);
        let station = self.free_station();
        let shooter = Shooter {
            entity: 1000 + player.0,
            name,
            station,
            stats: ShooterStats::default(),
        };
        let eye = shooter.eye();
        let entity = shooter.entity;
        self.shooters.insert(player, shooter);
        ctx.set_view(player, View::new(0.0, 0.0, 100.0));
        send_json_event(
            ctx,
            player,
            EVENT_HELLO,
            &json!({
                "entity": entity,
                "eye": eye,
                "maxRewindMs": MAX_REWIND_MS,
                "range": RANGE,
                "half": HALF,
            }),
        );
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.shooters.remove(&player);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<ShotInput> {
        ShotInput::decode(payload).filter(|s| s.flags & FLAG_FIRE != 0)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        // Shots first, against the history up to the previous tick (what
        // every client has been sent so far).
        let players: Vec<PlayerId> = self.shooters.keys().copied().collect();
        for player in players {
            for input in ctx.take_inputs(player, ConsumePolicy::Drain) {
                let Some(verdict) = self.resolve_shot(ctx, player, input.client_tick, &input.input)
                else {
                    continue;
                };
                let shooter = self.shooters.get_mut(&player).expect("present");
                shooter.stats.shots += 1;
                shooter.stats.clamped += u32::from(verdict.clamped);
                if let Some(id) = verdict.hit {
                    shooter.stats.hits += 1;
                    let flash = &mut self.flash[(id - 1) as usize];
                    *flash = flash.wrapping_add(1);
                }
                send_json_event(
                    ctx,
                    player,
                    EVENT_SHOT,
                    &json!({
                        "shot": verdict.shot,
                        "hit": verdict.hit,
                        "compensated": verdict.compensated,
                        "ageMs": verdict.age.as_secs_f64() * 1000.0,
                        "requestedAgeMs": verdict.requested_age.as_secs_f64() * 1000.0,
                        "clamped": verdict.clamped,
                        "tick": ctx.tick(),
                    }),
                );
            }
        }
        let t = ctx.sim_time().as_secs_f64();
        for (i, pos) in self.targets.iter_mut().enumerate() {
            *pos = target_position(i, t);
        }
    }

    fn record_rewind(&self, frame: &mut RewindFrame) {
        for (i, pos) in self.targets.iter().enumerate() {
            frame.push(Historic::sphere(i as u32 + 1, *pos, target_radius(i)));
        }
    }

    fn replicate(&self, out: &mut WorldView) {
        for s in self.shooters.values() {
            let (x, y) = station_position(s.station);
            out.push(EntityState::new(
                s.entity,
                KIND_SHOOTER,
                x,
                y,
                vec![
                    FieldValue::F(x),
                    FieldValue::F(y),
                    FieldValue::Str(s.name.clone()),
                    FieldValue::U(s.stats.shots),
                    FieldValue::U(s.stats.hits),
                ],
            ));
        }
        for (i, p) in self.targets.iter().enumerate() {
            out.push(
                EntityState::new(
                    i as u32 + 1,
                    KIND_TARGET,
                    p[0],
                    p[1],
                    vec![
                        FieldValue::F(p[0]),
                        FieldValue::F(p[1]),
                        FieldValue::F(p[2]),
                        FieldValue::F(target_radius(i)),
                        FieldValue::U(self.flash[i].into()),
                    ],
                )
                .global(),
            );
        }
    }
}

/// Demo registration (`game-demos gallery`).
pub struct GalleryDemo;

impl Demo for GalleryDemo {
    type Zone = Gallery;
    const NAME: &'static str = "gallery";
    const DEFAULT_ZONE: &'static str = "gallery-1";

    fn hosts_zone(zone_id: &str) -> bool {
        zone_id.starts_with("gallery-")
    }

    fn server(mut config: ServerConfig, guests: Guests) -> GameServer<Gallery> {
        config.host.rewind.max_rewind = Duration::from_millis(MAX_REWIND_MS);
        GameServer::new(config, move |zone_id| {
            Self::hosts_zone(zone_id).then(|| GalleryConfig {
                guests: guests.clone(),
            })
        })
    }
}
