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
//! Kinds are `#[derive(Replicated)]` views; inputs are shots only
//! ([`ShotInput`], a `#[derive(GameInput)]`: flags, shot id, view delay and
//! a 16-bit quantized unit direction). The shooter gets one
//! [`GalleryEvent::Shot`] per shot with the verdict and the rewind used. The
//! browser client is generated from [`game_surface`].

use std::collections::BTreeMap;
use std::time::Duration;

use distributed_game::net::{GameServer, ServerConfig};
use distributed_game::rewind::{Historic, HitQuery, RewindFrame, ShotTiming, Vec3};
use distributed_game::{
    schema, ConsumePolicy, Ctx, EntitySchema, FieldType, GameEvent, GameInput, GameSurface,
    LeaveReason, PlayerId, Replicated, View, WorldView, Zone,
};

use crate::common::{Demo, Guests};
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
pub const FLAG_FIRE: u8 = ShotInput::FIRE;
/// Input flag: evaluate without lag compensation.
pub const FLAG_NO_COMP: u8 = ShotInput::NO_COMP;

const POS: FieldType = FieldType::quant_centered(HALF, 18);

/// A shooter at its station (positions never change while connected).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "shooter", id = 1, priority = 0.5)]
pub struct ShooterView {
    /// Station x.
    #[field_type(POS)]
    pub x: f32,
    /// Station y.
    #[field_type(POS)]
    pub y: f32,
    /// Display name.
    #[priority(0.2)]
    pub name: String,
    /// Shots resolved.
    pub shots: u32,
    /// Shots that hit.
    pub hits: u32,
}

/// A moving target (global).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "target", id = 2, priority = 2.0, global)]
pub struct TargetView {
    /// Position x.
    #[field_type(POS)]
    pub x: f32,
    /// Position y.
    #[field_type(POS)]
    pub y: f32,
    /// Height.
    #[quant(min = 0.0, max = 4.0, bits = 12)]
    pub z: f32,
    /// Radius.
    pub r: f32,
    /// Bumps on every confirmed hit.
    pub flash: u8,
}

/// The gallery's events.
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum GalleryEvent {
    /// To a joining shooter.
    #[game_event(id = 1)]
    Hello {
        /// Own entity id.
        entity: u32,
        /// Eye position `[x, y, z]` (z up).
        eye: Vec<f32>,
        /// Furthest rewind (anti-cheat clamp).
        max_rewind_ms: u32,
        /// Hitscan range (m).
        range: f32,
        /// Floor half extent (m).
        half: f32,
    },
    /// The verdict of one shot.
    #[game_event(id = 2)]
    Shot {
        /// Client shot id.
        shot: u16,
        /// Target hit.
        hit: Option<u32>,
        /// Lag compensation used.
        compensated: bool,
        /// Rewind applied (0 without compensation).
        age_ms: f64,
        /// Rewind the client asked for.
        requested_age_ms: f64,
        /// Limited by the clamp / stored history.
        clamped: bool,
        /// Server tick of the verdict.
        tick: u32,
    },
}

/// The gallery's client surface.
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<Gallery>("gallery").events::<GalleryEvent>()
}

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

/// One shot (`#[derive(GameInput)]`): flags, shot id, view delay and the
/// aim direction quantized to 16 bits per axis (centered, so a zero axis is
/// exact; the zone re-normalizes, see [`ShotInput::parse`]).
#[derive(Debug, Clone, Copy, PartialEq, GameInput)]
pub struct ShotInput {
    /// [`ShotInput::FIRE`] | [`ShotInput::NO_COMP`].
    #[buttons(fire, no_comp)]
    pub flags: u8,
    /// Client shot id (echoed in the verdict).
    pub shot: u16,
    /// Interpolation delay behind `client_tick` the shooter was rendering (ms).
    pub delay_ms: u16,
    /// Aim direction x (game coordinates, z up).
    #[quant(centered = 1.0, bits = 16)]
    pub dx: f32,
    /// Aim direction y.
    #[quant(centered = 1.0, bits = 16)]
    pub dy: f32,
    /// Aim direction z.
    #[quant(centered = 1.0, bits = 16)]
    pub dz: f32,
}

impl ShotInput {
    /// A shot along `dir` (normalized here).
    pub fn new(flags: u8, shot: u16, delay_ms: u16, dir: Vec3) -> Self {
        let len = (dir[0] * dir[0] + dir[1] * dir[1] + dir[2] * dir[2])
            .sqrt()
            .max(f32::EPSILON);
        Self {
            flags,
            shot,
            delay_ms,
            dx: dir[0] / len,
            dy: dir[1] / len,
            dz: dir[2] / len,
        }
    }

    /// The aim direction.
    pub fn dir(&self) -> Vec3 {
        [self.dx, self.dy, self.dz]
    }

    /// Decode, sanitize and re-normalize a payload (the zone's decode);
    /// `None` for a malformed frame or a (near-)zero direction.
    pub fn parse(payload: &[u8]) -> Option<Self> {
        Self::from_payload(payload).and_then(Self::normalized)
    }

    /// Re-normalized after quantization; `None` for a (near-)zero direction.
    pub fn normalized(self) -> Option<Self> {
        let [x, y, z] = self.dir();
        let len = (x * x + y * y + z * z).sqrt();
        (len > 0.5).then(|| Self {
            dx: x / len,
            dy: y / len,
            dz: z / len,
            ..self
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
        let query = HitQuery::ray(shooter.eye(), shot.dir(), RANGE);
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
        schema![ShooterView, TargetView]
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
        ctx.send(
            player,
            &GalleryEvent::Hello {
                entity,
                eye: eye.to_vec(),
                max_rewind_ms: MAX_REWIND_MS as u32,
                range: RANGE,
                half: HALF,
            },
        );
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.shooters.remove(&player);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<ShotInput> {
        ShotInput::parse(payload).filter(|s| s.flags & FLAG_FIRE != 0)
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
                let tick = ctx.tick() as u32;
                ctx.send(
                    player,
                    &GalleryEvent::Shot {
                        shot: verdict.shot,
                        hit: verdict.hit,
                        compensated: verdict.compensated,
                        age_ms: verdict.age.as_secs_f64() * 1000.0,
                        requested_age_ms: verdict.requested_age.as_secs_f64() * 1000.0,
                        clamped: verdict.clamped,
                        tick,
                    },
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
            let view = ShooterView {
                x,
                y,
                name: s.name.clone(),
                shots: s.stats.shots,
                hits: s.stats.hits,
            };
            out.add(s.entity, &view);
        }
        for (i, p) in self.targets.iter().enumerate() {
            let view = TargetView {
                x: p[0],
                y: p[1],
                z: p[2],
                r: target_radius(i),
                flash: self.flash[i],
            };
            out.add(i as u32 + 1, &view);
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
