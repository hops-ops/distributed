//! Mob Pit's input frame, its client-predicted step (the Arena's
//! movement; combat is server-owned and never predicted) and its rooms.
//!
//! **Rooms.** Mob Pit is a near-infinite grid of rooms, room `(x, y)` being
//! zone `pit.<x>.<y>` (canonical integers, |x|, |y| ≤ [`MAX_ROOM`]). Every
//! room is the Arena floor (same pillars) with a doorway in the middle of
//! each wall leading to the neighbour.
//!
//! **One continuous world.** Room `(x, y)` sits at world `(x, y) ×`
//! [`ROOM_SIZE`]; neighbours share a wall. Positions on the wire are
//! room-local (centred), so world = room × [`ROOM_SIZE`] + local. A doorway
//! is a passage: [`pit_step`] lets the player walk through the gap past the
//! wall line (up to [`PASSAGE`] m); once it is [`HYSTERESIS`] m past the
//! line ([`crossing`]) the room hands it to the neighbour, which admits it
//! at the same world point with the same velocity ([`into_neighbour`]), so
//! nothing jumps and the client's prediction carries on (the Endless Grid's
//! model). What lives in a room — its biome, danger level and mobs — is
//! a pure function of `(seed, x, y)` ([`pit_room`], `#[game_pure]`) built on
//! the Endless Grid's seeded hashing and biomes ([`crate::grid`]); the page
//! runs the same code compiled to WebAssembly (minimap colours, floor).

use std::cell::RefCell;

use distributed_game::{game_pure, predict, FieldType, GameInput, GameRecord};

use crate::arena::{self, MoveInput, MoveState};
use crate::grid;

/// Input: the Arena's frame (`[mx, my, buttons]`) with attack, jump, fire,
/// no-compensation and aim bits next to dash, then the aim — the yaw (the
/// facing in the room's plane, radians from +x) and pitch (up positive) the
/// player looks along — and, for a ranged shot, its id and the view delay
/// behind `client_tick` the shooter was rendering (lag compensation). 11
/// bytes per command frame.
#[derive(Debug, Clone, Copy, Default, PartialEq, GameInput)]
pub struct PitInput {
    /// Stick x.
    #[range(min = -127, max = 127)]
    pub mx: i8,
    /// Stick y.
    #[range(min = -127, max = 127)]
    pub my: i8,
    /// [`PitInput::DASH`] | [`PitInput::ATTACK`] | [`PitInput::JUMP`] |
    /// [`PitInput::FIRE`] (a ranged shot) | [`PitInput::NO_COMP`] (resolve it
    /// without lag compensation) | [`PitInput::AIM`] (face the aim: first
    /// person).
    #[buttons(dash, attack, jump, fire, no_comp, aim)]
    pub buttons: u8,
    /// Aim yaw (radians from +x in the room's plane).
    #[quant(centered = 3.2, bits = 16)]
    pub yaw: f32,
    /// Aim pitch (radians, up positive).
    #[quant(centered = 1.6, bits = 16)]
    pub pitch: f32,
    /// Client shot id (with [`PitInput::FIRE`]; echoed in the verdict).
    pub shot: u16,
    /// Interpolation delay behind `client_tick` the shooter was rendering (ms).
    pub delay_ms: u16,
}

impl PitInput {
    /// The movement part (the Arena's step).
    pub fn movement(self) -> MoveInput {
        MoveInput {
            mx: self.mx,
            my: self.my,
            buttons: self.buttons & MoveInput::DASH,
        }
    }

    /// Attack held.
    pub fn attack(self) -> bool {
        self.buttons & Self::ATTACK != 0
    }

    /// Jump held.
    pub fn jump(self) -> bool {
        self.buttons & Self::JUMP != 0
    }

    /// A ranged shot this frame.
    pub fn fire(self) -> bool {
        self.buttons & Self::FIRE != 0
    }

    /// Face the aim (first person).
    pub fn aims(self) -> bool {
        self.buttons & Self::AIM != 0
    }

    /// The unit aim direction `[x, y, z]` (z up), from yaw and pitch.
    /// Server-side only (trig is not part of the predicted step).
    pub fn aim_dir(self) -> [f32; 3] {
        let (yaw, pitch) = (
            if self.yaw.is_finite() { self.yaw } else { 0.0 },
            if self.pitch.is_finite() {
                self.pitch.clamp(-1.5, 1.5)
            } else {
                0.0
            },
        );
        let flat = pitch.cos();
        [yaw.cos() * flat, yaw.sin() * flat, pitch.sin()]
    }
}

/// Gravity (m/s²).
pub const GRAVITY: f32 = 24.0;
/// Take-off speed of a jump (m/s): about 1.5 m up, 0.7 s in the air.
pub const JUMP_SPEED: f32 = 8.5;
/// Wire type of `z` (height above the floor, m).
pub const HEIGHT: FieldType = FieldType::quant_centered(4.0, 14);

/// Mob Pit's predicted state: the Arena capsule on the floor plus a
/// vertical axis (`z` above the floor, `vz`). Every field is replicated
/// under the same name, so clients rebase on the authoritative copy.
#[derive(Debug, Clone, Copy, Default, PartialEq, GameRecord)]
pub struct PitState {
    /// Position x.
    pub x: f32,
    /// Position y.
    pub y: f32,
    /// Velocity x.
    pub vx: f32,
    /// Velocity y.
    pub vy: f32,
    /// Remaining dash frames.
    pub dash: u8,
    /// Remaining dash cooldown frames.
    pub cd: u8,
    /// Height above the floor (m); 0 on the ground.
    pub z: f32,
    /// Vertical velocity (m/s).
    pub vz: f32,
}

impl PitState {
    /// The floor part (the Arena's state).
    pub fn planar(self) -> MoveState {
        MoveState {
            x: self.x,
            y: self.y,
            vx: self.vx,
            vy: self.vy,
            dash: self.dash,
            cd: self.cd,
        }
    }

    /// `self` with its floor part replaced.
    pub fn with_planar(self, m: MoveState) -> Self {
        Self {
            x: m.x,
            y: m.y,
            vx: m.vx,
            vy: m.vy,
            dash: m.dash,
            cd: m.cd,
            ..self
        }
    }

    /// Standing on the floor.
    pub fn grounded(self) -> bool {
        self.z <= 0.0
    }
}

/// `(seed, x, y)` → that room's obstacles as `(x, y, radius)`.
type ObstacleCache = Option<((u32, i32, i32), Vec<(f32, f32, f32)>)>;

thread_local! {
    /// Obstacles of the last room stepped in (a player stays in a room for
    /// many frames).
    static OBSTACLES: RefCell<ObstacleCache> = const { RefCell::new(None) };
}

/// The own capsule's predicted step: the Arena's movement of
/// [`PitInput::movement`] among the room's seeded obstacles, inside walls
/// with doorway passages. The context names the room, so the obstacles are
/// generated inside the step (the browser needs no copy of the room).
#[predict(PitStep, kind = "player")]
pub fn pit_step(
    state: &PitState,
    input: &PitInput,
    context: &PitRoomContext,
    _dt: f32,
) -> PitState {
    let key = (context.seed, context.x, context.y);
    OBSTACLES.with(|cache| {
        let mut cache = cache.borrow_mut();
        if cache.as_ref().map(|(k, _)| *k) != Some(key) {
            let circles = room_obstacles(context.seed, (context.x, context.y))
                .1
                .iter()
                .map(|o| (o.x, o.y, o.r))
                .collect();
            *cache = Some((key, circles));
        }
        let obstacles = &cache.as_ref().expect("filled above").1;
        step_among(*state, *input, obstacles)
    })
}

/// Room edge length (m): rooms tile the world, sharing walls.
pub const ROOM_SIZE: f32 = 2.0 * arena::HALF;
/// How far past the wall line a doorway passage reaches (m).
pub const PASSAGE: f32 = 2.0;
/// Handoff hysteresis (m): a player belongs to the neighbour once it is
/// this far past the wall line, and so arrives this far inside it.
pub const HYSTERESIS: f32 = 0.5;
/// Tolerance of the wall tests (m), far above the wire precision.
const SLACK: f32 = 0.01;
/// Wire type of player `x`/`y`: the room plus its passages, ≈ 0.17 mm steps.
pub const POS: FieldType = FieldType::quant_centered(22.0, 18);

/// One command frame in a room with the classic pillars (the entrance).
pub fn step(s: PitState, input: PitInput) -> PitState {
    step_among(s, input, &arena::PILLARS)
}

/// One command frame: [`arena::advance_among`] (stick, dash, the room's
/// obstacles), then the walls with their doorway passages, then the jump
/// (take off from the floor, gravity, land), snapped to the wire precision.
pub fn step_among(s: PitState, input: PitInput, obstacles: &[(f32, f32, f32)]) -> PitState {
    let planar = s.planar();
    let next = walls(
        planar,
        arena::advance_among(planar, input.movement(), obstacles),
    );
    let (mut z, mut vz) = (s.z, s.vz);
    if input.jump() && s.grounded() {
        vz = JUMP_SPEED;
    }
    if z > 0.0 || vz > 0.0 {
        z += vz * arena::DT;
        vz -= GRAVITY * arena::DT;
        if z <= 0.0 {
            z = 0.0;
            vz = 0.0;
        }
    }
    canonicalize(PitState { z, vz, ..s }.with_planar(next))
}

/// Snap to the replicated precision of a Mob Pit player.
pub fn canonicalize(s: PitState) -> PitState {
    PitState {
        x: arena::snap(POS, s.x),
        y: arena::snap(POS, s.y),
        vx: arena::snap(arena::VEL, s.vx),
        vy: arena::snap(arena::VEL, s.vy),
        z: arena::snap(HEIGHT, s.z).max(0.0),
        vz: arena::snap(arena::VEL, s.vz),
        ..s
    }
}

/// Clamp a coordinate to `±lim`, stopping the velocity into the wall.
fn clamp_axis(p: f32, v: f32, lim: f32) -> (f32, f32) {
    if p > lim {
        (lim, v.min(0.0))
    } else if p < -lim {
        (-lim, v.max(0.0))
    } else {
        (p, v)
    }
}

/// Keep `s` (one frame after `prev`) inside the room square or a doorway
/// passage. A player already in a passage stays between its jambs; one in
/// the room may enter a passage only through the gap.
fn walls(prev: MoveState, s: MoveState) -> MoveState {
    let lim = arena::HALF - arena::PLAYER_RADIUS;
    // Snapped to the wire, a player held at the wall may sit a hair past it.
    let inside = lim + SLACK;
    let gap = DOOR_HALF - arena::PLAYER_RADIUS;
    let far = arena::HALF + PASSAGE;
    let mut s = s;
    let (lim_x, lim_y) = if prev.x.abs() > inside {
        (far, gap)
    } else if prev.y.abs() > inside {
        (gap, far)
    } else {
        (
            if s.y.abs() <= gap { far } else { lim },
            if s.x.abs() <= gap { far } else { lim },
        )
    };
    (s.x, s.vx) = clamp_axis(s.x, s.vx, lim_x);
    (s.y, s.vy) = clamp_axis(s.y, s.vy, lim_y);
    s
}

/// Largest |room coordinate|.
pub const MAX_ROOM: i32 = 1_000_000;
/// Half width of a doorway (m), centred on each wall.
pub const DOOR_HALF: f32 = 2.5;
/// Default world seed of the rooms.
pub const DEFAULT_SEED: u32 = 0x0B1D_5EED;
/// Mobs a room holds at most (12 slots, plus a boss room's boss).
pub const MAX_MOBS: usize = 13;
/// Mob tier of a hood's boss.
pub const BOSS_TIER: u8 = 4;
/// Where a boss stands (room-local), between the central pillar and the
/// south door.
pub const BOSS_HOME: (f32, f32) = (0.0, -7.0);
/// Rooms per hood side: a hood is 3 × 3 rooms.
pub const HOOD_ROOMS: i32 = 3;
/// Hoods per borough side: a borough is 3 × 3 hoods (9 × 9 rooms).
pub const BOROUGH_HOODS: i32 = 3;
/// Hash salt of the boss rooms.
const SALT_BOSS: u32 = 901;

/// Unit directions of the 12 mob slots (angles `(k + 0.5) / 12 · τ`), as
/// literals so native and WebAssembly agree bit for bit (no `sin`/`cos`).
const SLOTS: [(f32, f32); 12] = [
    (0.9659258, 0.258819),
    (
        std::f32::consts::FRAC_1_SQRT_2,
        std::f32::consts::FRAC_1_SQRT_2,
    ),
    (0.258819, 0.9659258),
    (-0.258819, 0.9659258),
    (
        -std::f32::consts::FRAC_1_SQRT_2,
        std::f32::consts::FRAC_1_SQRT_2,
    ),
    (-0.9659258, 0.258819),
    (-0.9659258, -0.258819),
    (
        -std::f32::consts::FRAC_1_SQRT_2,
        -std::f32::consts::FRAC_1_SQRT_2,
    ),
    (-0.258819, -0.9659258),
    (0.258819, -0.9659258),
    (
        std::f32::consts::FRAC_1_SQRT_2,
        -std::f32::consts::FRAC_1_SQRT_2,
    ),
    (0.9659258, -0.258819),
];

/// Hash salt of the room contents.
const SALT_ROOM: u32 = 900;

/// A round obstacle of a room (room-local): a column, a boulder, a tree.
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct Obstacle {
    /// Centre x (m, room-local).
    pub x: f32,
    /// Centre y.
    pub y: f32,
    /// Radius.
    pub r: f32,
    /// What it looks like: 0 column · 1 boulder · 2 tree · 3 crystal.
    pub look: u8,
}

/// Room patterns, by index ([`PitRoomLayout::pattern`]).
pub const PATTERNS: [&str; 6] = ["pillars", "colonnade", "ring", "rubble", "hall", "grove"];
/// Most obstacles a room has.
pub const MAX_OBSTACLES: usize = 24;
/// Hash salt of the room obstacles.
const SALT_OBSTACLE: u32 = 902;
/// Half width of the clear corridor along each door axis near the walls.
const CORRIDOR: f32 = 3.2;
/// Free gap kept between obstacles, and between an obstacle and a wall or
/// a kept spot (a player's diameter plus slack): every room stays one
/// connected floor.
const GAP: f32 = 1.3;

/// Whether `(x, y, r)` keeps clear of the door corridors, the walls and the
/// `keep` spots (`(x, y, clearance)`) and of `placed`.
fn fits(x: f32, y: f32, r: f32, keep: &[(f32, f32, f32)], placed: &[Obstacle]) -> bool {
    let wall = arena::HALF - r - GAP;
    if x.abs() > wall || y.abs() > wall {
        return false;
    }
    // Door corridors: from each doorway 8 m into the room.
    let inner = arena::HALF - 8.0;
    if (x.abs() < CORRIDOR + r && y.abs() > inner - r)
        || (y.abs() < CORRIDOR + r && x.abs() > inner - r)
    {
        return false;
    }
    keep.iter().all(|&(kx, ky, c)| {
        let (dx, dy) = (x - kx, y - ky);
        dx * dx + dy * dy >= (r + c) * (r + c)
    }) && placed.iter().all(|o| {
        let (dx, dy) = (x - o.x, y - o.y);
        let min = r + o.r + GAP;
        dx * dx + dy * dy >= min * min
    })
}

/// Unit vectors at `(k + 0.5) / 8 · τ`, as literals (no trig: native and
/// WebAssembly agree bit for bit).
const RING8: [(f32, f32); 8] = [
    (0.9238795, 0.38268343),
    (0.38268343, 0.9238795),
    (-0.38268343, 0.9238795),
    (-0.9238795, 0.38268343),
    (-0.9238795, -0.38268343),
    (-0.38268343, -0.9238795),
    (0.38268343, -0.9238795),
    (0.9238795, -0.38268343),
];

/// Spots every room keeps clear: the spawn points and the boss's home.
const KEEP: [(f32, f32, f32); 7] = [
    (-4.5, -4.5, 1.6),
    (4.5, -4.5, 1.6),
    (4.5, 4.5, 1.6),
    (-4.5, 4.5, 1.6),
    (0.0, -5.5, 1.6),
    (0.0, 5.5, 1.6),
    (BOSS_HOME.0, BOSS_HOME.1, 2.6),
];

/// The obstacles of room `room` and its pattern ([`PATTERNS`]): the
/// entrance has the classic pillars; every other room one seeded pattern,
/// each obstacle kept clear of the door corridors, the walls, the spawn
/// points, the mob homes and the other obstacles (by at least a player's
/// width), so the floor stays one connected place. Pure and deterministic.
pub fn room_obstacles(seed: u32, room: (i32, i32)) -> (u8, Vec<Obstacle>) {
    let classic = || {
        arena::PILLARS
            .iter()
            .map(|&(x, y, r)| Obstacle { x, y, r, look: 0 })
            .collect::<Vec<_>>()
    };
    if room == (0, 0) {
        return (0, classic());
    }
    let h = grid::mix(seed, SALT_OBSTACLE, room.0, room.1);
    let pattern = (h % PATTERNS.len() as u32) as u8;
    if pattern == 0 {
        return (0, classic());
    }
    let mut keep: Vec<(f32, f32, f32)> = KEEP.to_vec();
    for m in mob_slots(seed, room) {
        keep.push((m.x, m.y, 0.8 + GAP));
    }
    let mut placed: Vec<Obstacle> = Vec::new();
    let place = |x: f32, y: f32, r: f32, look: u8, placed: &mut Vec<Obstacle>| {
        if placed.len() < MAX_OBSTACLES && fits(x, y, r, &keep, placed) {
            placed.push(Obstacle { x, y, r, look });
        }
    };
    let roll = |i: u32| grid::unit(grid::hash32(h ^ i.wrapping_mul(0x9e37_79b9)));
    match pattern {
        // Two rows of columns along one axis.
        1 => {
            let along_x = h & 0x100 != 0;
            for row in [-6.5f32, 6.5] {
                let mut t = -15.0f32;
                while t <= 15.0 {
                    let (x, y) = if along_x { (t, row) } else { (row, t) };
                    place(x, y, 0.8, 0, &mut placed);
                    t += 3.75;
                }
            }
        }
        // A ring of columns around a central plinth.
        2 => {
            place(0.0, 0.0, 2.0, 0, &mut placed);
            let radius = 8.5 + 2.0 * roll(1);
            for (ux, uy) in RING8 {
                place(ux * radius, uy * radius, 1.0, 0, &mut placed);
            }
        }
        // Scattered boulders.
        3 => {
            for i in 0..40u32 {
                let x = -17.0 + 34.0 * roll(2 * i + 2);
                let y = -17.0 + 34.0 * roll(2 * i + 3);
                let r = 0.6 + 1.2 * roll(100 + i);
                place(x, y, r, 1, &mut placed);
            }
        }
        // A rounded hall: boulders in the corners, four columns.
        4 => {
            for (sx, sy) in [(1.0f32, 1.0f32), (-1.0, 1.0), (-1.0, -1.0), (1.0, -1.0)] {
                place(sx * 15.0, sy * 15.0, 3.5, 1, &mut placed);
                place(sx * 6.5, sy * 6.5, 1.0, 0, &mut placed);
            }
            place(0.0, 0.0, 1.5, 3, &mut placed);
        }
        // A grove of trees.
        _ => {
            for i in 0..60u32 {
                let x = -17.5 + 35.0 * roll(2 * i + 7);
                let y = -17.5 + 35.0 * roll(2 * i + 8);
                let r = 0.45 + 0.4 * roll(200 + i);
                place(x, y, r, 2, &mut placed);
            }
        }
    }
    (pattern, placed)
}

/// Which room ([`pit_room`]'s input).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, GameRecord)]
pub struct PitRoomContext {
    /// World seed.
    pub seed: u32,
    /// Room x.
    pub x: i32,
    /// Room y.
    pub y: i32,
}

/// One mob slot of a room: its tier and home.
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct RoomMob {
    /// 1 grunt · 2 brute · 3 elite.
    pub tier: u8,
    /// Home x (m, room-local).
    pub x: f32,
    /// Home y (m, room-local).
    pub y: f32,
}

/// What a room holds.
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct PitRoomLayout {
    /// Biome index into [`grid::BIOMES`] (the room's look).
    pub biome: u8,
    /// Danger 1..=3: grows with the distance from the entrance room.
    pub danger: u8,
    /// Mob slots (ids 1..); a boss room's last slot is its boss
    /// ([`BOSS_TIER`]).
    pub mobs: Vec<RoomMob>,
    /// This is its hood's boss room.
    pub boss: bool,
    /// Hood x (see [`hood_of`]).
    pub hood_x: i32,
    /// Hood y.
    pub hood_y: i32,
    /// Rooms to the nearest boss room (Chebyshev; 0 in a boss room, at most
    /// [`BOSS_DISTANCE_MAX`]): fog and darkness thicken toward bosses.
    pub boss_distance: u8,
    /// Index into [`PATTERNS`]: how the room is laid out.
    pub pattern: u8,
    /// The room's obstacles (collision in the step, drawn by the pages).
    pub obstacles: Vec<Obstacle>,
}

/// The farthest [`PitRoomLayout::boss_distance`] reported.
pub const BOSS_DISTANCE_MAX: u8 = 6;

/// The hood of room `room` (3 × 3 rooms each, floor division).
pub fn hood_of(room: (i32, i32)) -> (i32, i32) {
    (room.0.div_euclid(HOOD_ROOMS), room.1.div_euclid(HOOD_ROOMS))
}

/// The borough of hood `hood` (3 × 3 hoods each).
pub fn borough_of(hood: (i32, i32)) -> (i32, i32) {
    (
        hood.0.div_euclid(BOROUGH_HOODS),
        hood.1.div_euclid(BOROUGH_HOODS),
    )
}

/// The 9 hoods of borough `borough`.
pub fn borough_hoods(borough: (i32, i32)) -> Vec<(i32, i32)> {
    let mut hoods = Vec::with_capacity(9);
    for dy in 0..BOROUGH_HOODS {
        for dx in 0..BOROUGH_HOODS {
            hoods.push((
                borough.0 * BOROUGH_HOODS + dx,
                borough.1 * BOROUGH_HOODS + dy,
            ));
        }
    }
    hoods
}

/// `x.y`: the cell shard of a hood or borough.
pub fn pair_shard(at: (i32, i32)) -> String {
    format!("{}.{}", at.0, at.1)
}

/// Parse `x.y` (canonical integers within [`MAX_ROOM`]).
pub fn parse_pair(shard: &str) -> Option<(i32, i32)> {
    let (x, y) = shard.split_once('.')?;
    let parse = |s: &str| -> Option<i32> {
        let v: i32 = s.parse().ok()?;
        (v.to_string() == s && v.abs() <= MAX_ROOM).then_some(v)
    };
    Some((parse(x)?, parse(y)?))
}

/// The boss room of hood `hood`: one of its 9 rooms, seeded (never the
/// entrance `pit.0.0`).
pub fn boss_room(seed: u32, hood: (i32, i32)) -> (i32, i32) {
    let h = grid::mix(seed, SALT_BOSS, hood.0, hood.1);
    let pick = |k: i32| {
        (
            hood.0 * HOOD_ROOMS + k % HOOD_ROOMS,
            hood.1 * HOOD_ROOMS + k / HOOD_ROOMS,
        )
    };
    let room = pick((h % 9) as i32);
    if room == (0, 0) {
        pick(((h % 9) as i32 + 1) % 9)
    } else {
        room
    }
}

/// Rooms from `room` to the nearest boss room (Chebyshev, capped at
/// [`BOSS_DISTANCE_MAX`]).
pub fn boss_distance(seed: u32, room: (i32, i32)) -> u8 {
    let (hx, hy) = hood_of(room);
    let mut best = u32::from(BOSS_DISTANCE_MAX);
    for dy in -2..=2 {
        for dx in -2..=2 {
            let b = boss_room(seed, (hx + dx, hy + dy));
            let d = (b.0 - room.0)
                .unsigned_abs()
                .max((b.1 - room.1).unsigned_abs());
            best = best.min(d);
        }
    }
    best as u8
}

/// The contents of room `(x, y)`: biome from the Endless Grid's noise,
/// danger from the distance to `pit.0.0`, 6–12 mobs on two rings between
/// the pillars with tiers weighted by danger. Pure and deterministic.
#[game_pure(PitRoomFn)]
pub fn pit_room(room: &PitRoomContext) -> PitRoomLayout {
    let at = (room.x, room.y);
    let biome = grid::biome(room.seed, at, (grid::CELL / 2.0, grid::CELL / 2.0));
    let distance = room.x.unsigned_abs().saturating_add(room.y.unsigned_abs());
    let danger = (1 + distance / 3).min(3) as u8;
    let mut mobs = mob_slots(room.seed, at);
    let hood = hood_of(at);
    let boss = boss_room(room.seed, hood) == at;
    if boss {
        mobs.push(RoomMob {
            tier: BOSS_TIER,
            x: BOSS_HOME.0,
            y: BOSS_HOME.1,
        });
    }
    let (pattern, obstacles) = room_obstacles(room.seed, at);
    PitRoomLayout {
        biome,
        danger,
        mobs,
        boss,
        hood_x: hood.0,
        hood_y: hood.1,
        boss_distance: boss_distance(room.seed, at),
        pattern,
        obstacles,
    }
}

/// The 6–12 ordinary mob slots of a room: two rings between the pillars,
/// tiers weighted by the room's danger.
fn mob_slots(seed: u32, at: (i32, i32)) -> Vec<RoomMob> {
    let distance = at.0.unsigned_abs().saturating_add(at.1.unsigned_abs());
    let danger = (1 + distance / 3).min(3) as u8;
    let h = grid::mix(seed, SALT_ROOM, at.0, at.1);
    let count = 6 + (h % 7) as usize;
    let mut mobs = Vec::with_capacity(count);
    for i in 0..count {
        let roll = grid::hash32(h ^ (i as u32).wrapping_mul(0x9e37_79b9));
        let ring = if i % 2 == 0 { 11.5 } else { 16.0 };
        let (dx, dy) = SLOTS[i * SLOTS.len() / count];
        // Elites only where it is dangerous; brutes grow with danger.
        let tier = match (roll % 100, danger) {
            (r, 3) if r < 25 => 3,
            (r, 2) if r < 8 => 3,
            (r, d) if r < 15 + 20 * u32::from(d) => 2,
            _ => 1,
        };
        mobs.push(RoomMob {
            tier,
            x: ring * dx,
            y: ring * dy,
        });
    }
    mobs
}

/// The rooms' world, for the page (generated `PIT_WORLD`).
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct PitWorld {
    /// Default seed.
    pub default_seed: u32,
    /// Floor half extent (m).
    pub half: f32,
    /// Player capsule radius (m).
    pub player_radius: f32,
    /// Dash cooldown in command frames (the HUD meter).
    pub dash_cooldown: u8,
    /// The pillars every room has (room-local).
    pub pillars: Vec<arena::Pillar>,
    /// Doorway half width (m).
    pub door_half: f32,
    /// Room edge length (m): room `(x, y)` is centred at world `(x, y) × room_size`.
    pub room_size: f32,
    /// Doorway passage depth past the wall line (m).
    pub passage: f32,
    /// Handoff hysteresis past the wall line (m).
    pub hysteresis: f32,
    /// Largest |room coordinate|.
    pub max_room: i32,
    /// Biome names by index.
    pub biomes: Vec<String>,
}

/// The `PIT_WORLD` constant.
pub fn pit_world() -> PitWorld {
    PitWorld {
        default_seed: DEFAULT_SEED,
        half: arena::HALF,
        player_radius: arena::PLAYER_RADIUS,
        dash_cooldown: arena::DASH_COOLDOWN,
        pillars: arena::arena_floor().pillars,
        door_half: DOOR_HALF,
        room_size: ROOM_SIZE,
        passage: PASSAGE,
        hysteresis: HYSTERESIS,
        max_room: MAX_ROOM,
        biomes: grid::BIOMES.map(String::from).to_vec(),
    }
}

/// `pit.<x>.<y>`.
pub fn room_zone(room: (i32, i32)) -> String {
    format!("pit.{}.{}", room.0, room.1)
}

/// Parse `pit.<x>.<y>` (canonical integers within [`MAX_ROOM`]).
pub fn parse_room(zone_id: &str) -> Option<(i32, i32)> {
    let rest = zone_id.strip_prefix("pit.")?;
    let (x, y) = rest.split_once('.')?;
    let parse = |s: &str| -> Option<i32> {
        let v: i32 = s.parse().ok()?;
        (v.to_string() == s && v.abs() <= MAX_ROOM).then_some(v)
    };
    Some((parse(x)?, parse(y)?))
}

/// The neighbour a player at room-local `(x, y)` belongs to, as a room
/// offset: [`HYSTERESIS`] past a wall line (only reachable through a
/// doorway passage).
pub fn crossing(x: f32, y: f32) -> Option<(i32, i32)> {
    let line = arena::HALF + HYSTERESIS;
    if x > line {
        Some((1, 0))
    } else if x < -line {
        Some((-1, 0))
    } else if y > line {
        Some((0, 1))
    } else if y < -line {
        Some((0, -1))
    } else {
        None
    }
}

/// The same player in the frame of the neighbour at room offset `step`:
/// the same world point and velocity (world = room × [`ROOM_SIZE`] + local).
pub fn into_neighbour(step: (i32, i32), s: PitState) -> PitState {
    canonicalize(PitState {
        x: s.x - step.0 as f32 * ROOM_SIZE,
        y: s.y - step.1 as f32 * ROOM_SIZE,
        ..s
    })
}

/// Whether `s` is a position a player can be in (the room or a passage).
pub fn admissible(s: &PitState) -> bool {
    let lim = arena::HALF - arena::PLAYER_RADIUS + SLACK;
    let gap = DOOR_HALF - arena::PLAYER_RADIUS + SLACK;
    let far = arena::HALF + PASSAGE + SLACK;
    let (ax, ay) = (s.x.abs(), s.y.abs());
    s.x.is_finite()
        && s.y.is_finite()
        && s.vx.is_finite()
        && s.vy.is_finite()
        && s.z.is_finite()
        && s.vz.is_finite()
        && (0.0..=4.0).contains(&s.z)
        && ((ax <= lim && ay <= lim) || (ay <= gap && ax <= far) || (ax <= gap && ay <= far))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rooms_are_deterministic_and_vary() {
        let ctx = |x, y| PitRoomContext {
            seed: DEFAULT_SEED,
            x,
            y,
        };
        assert_eq!(pit_room(&ctx(3, -2)), pit_room(&ctx(3, -2)));
        let rooms: Vec<_> = (-3..=3).map(|x| pit_room(&ctx(x, 0))).collect();
        assert!(rooms.iter().all(|r| (6..=MAX_MOBS).contains(&r.mobs.len())));
        assert!(rooms.windows(2).any(|w| w[0] != w[1]));
        assert_eq!(pit_room(&ctx(0, 0)).danger, 1);
        assert_eq!(pit_room(&ctx(9, 9)).danger, 3);
        assert!(rooms
            .iter()
            .all(|r| (r.biome as usize) < grid::BIOMES.len()));
    }

    #[test]
    fn every_hood_has_one_boss_room() {
        for hy in -4..4 {
            for hx in -4..4 {
                let rooms: Vec<(i32, i32)> = (0..9)
                    .map(|k| (hx * 3 + k % 3, hy * 3 + k / 3))
                    .filter(|&(x, y)| {
                        pit_room(&PitRoomContext {
                            seed: DEFAULT_SEED,
                            x,
                            y,
                        })
                        .boss
                    })
                    .collect();
                assert_eq!(rooms, vec![boss_room(DEFAULT_SEED, (hx, hy))]);
                assert_eq!(hood_of(rooms[0]), (hx, hy));
                assert_ne!(rooms[0], (0, 0), "never the entrance");
            }
        }
        let b = boss_room(DEFAULT_SEED, (0, 0));
        let layout = pit_room(&PitRoomContext {
            seed: DEFAULT_SEED,
            x: b.0,
            y: b.1,
        });
        assert_eq!(layout.boss_distance, 0);
        assert_eq!(layout.mobs.last().map(|m| m.tier), Some(BOSS_TIER));
        assert_eq!(hood_of((-1, -1)), (-1, -1));
        assert_eq!(borough_of((-1, 5)), (-1, 1));
        assert_eq!(parse_pair(&pair_shard((-3, 7))), Some((-3, 7)));
        assert_eq!(borough_hoods((0, 0)).len(), 9);
    }

    /// Free floor points reachable from a spawn point (0.5 m lattice).
    fn reachable(obstacles: &[Obstacle]) -> Vec<Vec<bool>> {
        let n = 79usize; // -19.5 ..= 19.5
        let at = |i: usize| -19.5 + i as f32 * 0.5;
        let free = |x: f32, y: f32| {
            let lim = arena::HALF - arena::PLAYER_RADIUS;
            x.abs() <= lim
                && y.abs() <= lim
                && obstacles.iter().all(|o| {
                    let (dx, dy) = (x - o.x, y - o.y);
                    dx * dx + dy * dy >= (o.r + arena::PLAYER_RADIUS).powi(2)
                })
        };
        let mut seen = vec![vec![false; n]; n];
        let start = ((4.5 + 19.5) / 0.5) as usize;
        let mut stack = vec![(start, start)];
        while let Some((i, j)) = stack.pop() {
            if seen[i][j] || !free(at(i), at(j)) {
                continue;
            }
            seen[i][j] = true;
            if i > 0 {
                stack.push((i - 1, j));
            }
            if j > 0 {
                stack.push((i, j - 1));
            }
            if i + 1 < n {
                stack.push((i + 1, j));
            }
            if j + 1 < n {
                stack.push((i, j + 1));
            }
        }
        seen
    }

    #[test]
    fn rooms_vary_and_every_room_stays_one_floor() {
        let mut patterns = std::collections::BTreeSet::new();
        let cell = |v: f32| ((v + 19.5) / 0.5).round() as usize;
        for y in -6..=6 {
            for x in -6..=6 {
                let layout = pit_room(&PitRoomContext {
                    seed: DEFAULT_SEED,
                    x,
                    y,
                });
                patterns.insert(layout.pattern);
                assert!(layout.obstacles.len() <= MAX_OBSTACLES);
                let seen = reachable(&layout.obstacles);
                // Every doorway, spawn point and mob home is reachable.
                for (px, py) in [(18.5, 0.0), (-18.5, 0.0), (0.0, 18.5), (0.0, -18.5)]
                    .into_iter()
                    .chain(KEEP.iter().map(|k| (k.0, k.1)))
                {
                    assert!(
                        seen[cell(px)][cell(py)],
                        "pit.{x}.{y} ({}): {px},{py} cut off",
                        PATTERNS[layout.pattern as usize]
                    );
                }
                for m in &layout.mobs {
                    let near = [(0.0, 0.0), (1.0, 0.0), (-1.0, 0.0), (0.0, 1.0), (0.0, -1.0)]
                        .iter()
                        .any(|(dx, dy)| seen[cell(m.x + dx)][cell(m.y + dy)]);
                    assert!(near, "pit.{x}.{y}: mob home {},{} cut off", m.x, m.y);
                }
            }
        }
        assert!(patterns.len() >= 5, "{patterns:?}");
        assert_eq!(
            room_obstacles(DEFAULT_SEED, (0, 0)).0,
            0,
            "the entrance is classic"
        );
    }

    #[test]
    fn the_step_collides_with_the_rooms_obstacles() {
        // A room with a ring: walk from the centre outwards along a column.
        let (pattern, obstacles) = (1..40)
            .map(|x| room_obstacles(DEFAULT_SEED, (x, 3)))
            .find(|(p, o)| *p == 3 && !o.is_empty())
            .expect("a rubble room");
        assert_eq!(pattern, 3);
        let o = obstacles[0];
        let circles: Vec<(f32, f32, f32)> = obstacles.iter().map(|o| (o.x, o.y, o.r)).collect();
        let mut s = canonicalize(PitState {
            x: o.x - (o.r + 3.0),
            y: o.y,
            ..PitState::default()
        });
        for _ in 0..90 {
            s = step_among(
                s,
                PitInput {
                    mx: 127,
                    ..PitInput::default()
                },
                &circles,
            );
            let (dx, dy) = (s.x - o.x, s.y - o.y);
            assert!(
                dx * dx + dy * dy >= (o.r + arena::PLAYER_RADIUS - 1e-3).powi(2),
                "{s:?}"
            );
        }
    }

    #[test]
    fn a_jump_goes_up_and_lands() {
        let jump = PitInput {
            buttons: PitInput::JUMP,
            ..PitInput::default()
        };
        let mut s = canonicalize(PitState {
            x: 4.5,
            y: 4.5,
            ..PitState::default()
        });
        s = step(s, jump);
        assert!(s.z > 0.2 && s.vz > 0.0, "{s:?}");
        let mut apex = 0.0f32;
        let mut frames = 1;
        while !s.grounded() {
            // Holding jump in the air does nothing more.
            s = step(s, jump);
            apex = apex.max(s.z);
            frames += 1;
            assert!(frames < 60, "lands: {s:?}");
        }
        assert!((1.2..1.8).contains(&apex), "apex {apex}");
        assert_eq!((s.z, s.vz), (0.0, 0.0));
        assert!((18..26).contains(&frames), "{frames} frames in the air");
        // Through a doorway mid-air, the height is carried.
        let air = canonicalize(PitState {
            x: 20.6,
            y: 0.5,
            z: 0.8,
            vz: 2.0,
            ..PitState::default()
        });
        let n = into_neighbour((1, 0), air);
        assert_eq!((n.z, n.vz), (air.z, air.vz));
    }

    #[test]
    fn room_ids_and_doorways() {
        assert_eq!(room_zone((1, -2)), "pit.1.-2");
        assert_eq!(parse_room("pit.1.-2"), Some((1, -2)));
        assert_eq!(parse_room("pit.01.2"), None);
        assert_eq!(parse_room("pit-1"), None);
        assert_eq!(parse_room("pit.2000000.0"), None);
    }

    fn walk(mut s: PitState, mx: i8, my: i8, frames: usize) -> PitState {
        for _ in 0..frames {
            s = step(
                s,
                PitInput {
                    mx,
                    my,
                    ..PitInput::default()
                },
            );
        }
        s
    }

    #[test]
    fn doorways_are_passages_and_walls_hold() {
        let lim = arena::HALF - arena::PLAYER_RADIUS;
        // Along the east door axis (y = 0.5): through the gap past the wall
        // line, up to the passage's end.
        let at = |x, y| PitState {
            x,
            y,
            ..PitState::default()
        };
        let s = walk(at(17.0, 0.5), 127, 0, 60);
        assert!((s.x - (arena::HALF + PASSAGE)).abs() < 1e-3, "{s:?}");
        assert_eq!(crossing(s.x, s.y), Some((1, 0)));
        // Beside the doorway the wall holds.
        let s = walk(at(17.0, 6.0), 127, 0, 60);
        assert!((s.x - lim).abs() < 1e-3, "{s:?}");
        assert_eq!(crossing(s.x, s.y), None);
        // In the passage the jambs hold the player in the gap.
        let s = walk(at(20.4, 0.0), 0, 127, 60);
        assert!(s.y <= DOOR_HALF - arena::PLAYER_RADIUS + 1e-3, "{s:?}");
        assert!(admissible(&s));
        // Corners never open.
        let s = walk(at(18.0, 18.0), 127, 127, 90);
        assert!(s.x <= lim + 1e-3 && s.y <= lim + 1e-3, "{s:?}");
    }

    #[test]
    fn crossing_keeps_the_world_point() {
        let s = canonicalize(PitState {
            x: arena::HALF + HYSTERESIS + 0.1,
            y: 1.25,
            vx: 5.0,
            ..PitState::default()
        });
        let step = crossing(s.x, s.y).expect("past the line");
        let n = into_neighbour(step, s);
        // World x: room 0 + s.x == room 1 + n.x.
        assert!((n.x + ROOM_SIZE - s.x).abs() < 1e-3, "{n:?}");
        assert_eq!((n.y, n.vx), (s.y, s.vx));
        assert!(admissible(&n));
        assert_eq!(crossing(n.x, n.y), None, "hysteresis: no bounce back");
        // West / south mirror.
        let w = into_neighbour((-1, 0), PitState { x: -21.0, ..s });
        assert!((w.x - 19.0).abs() < 1e-3, "{w:?}");
        let south = into_neighbour(
            (0, -1),
            PitState {
                x: 0.0,
                y: -21.0,
                ..s
            },
        );
        assert!((south.y - 19.0).abs() < 1e-3, "{south:?}");
        assert!(!admissible(&PitState {
            x: 21.0,
            y: 5.0,
            ..s
        }));
    }
}
