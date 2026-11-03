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

use distributed_game::collide::{self, layer, Collider, Colliders, Heightfield, Ray, Uphill, HOLE};
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

/// Coyote time: a jump still works this many ticks after walking off
/// walkable ground (0.1 s: the most whole ticks under 0.12 s).
pub const COYOTE: u8 = 3;
/// A jump press is remembered this many ticks (0.1 s) and fires on the
/// first tick the player can jump.
pub const JUMP_BUFFER: u8 = 3;
/// [`PitState::air`] after a jump: the coyote window is used up.
pub const JUMPED: u8 = u8::MAX;
/// Ground movement (m/s²): to full speed ([`arena::MAX_SPEED`]) in 0.15 s,
/// to a stop in 0.1 s with no direction held, twice that turning against
/// the current velocity. Half each in the air.
pub const RATES: arena::Rates = arena::Rates {
    accel: arena::MAX_SPEED / 0.15,
    decel: arena::MAX_SPEED / 0.1,
    reverse: 2.0 * arena::MAX_SPEED / 0.1,
};

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
    /// Ticks since the feet last stood on walkable ground (saturating;
    /// [`JUMPED`] once a jump used the coyote window).
    pub air: u8,
    /// Jump buffer: ticks a jump press stays wanted ([`JUMP_BUFFER`]).
    pub buf: u8,
    /// The jump button was held last tick (a press is its first tick).
    pub held: bool,
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

    /// Standing on the room's floor (`z = 0`; a ramp is above it).
    pub fn grounded(self) -> bool {
        self.z <= 0.0
    }
}

/// `(seed, x, y)` → that room's obstacles as `(x, y, radius)`, its
/// ground ([`room_ground`]) and its [`open_sides`].
type ObstacleCache = Option<((u32, i32, i32), (Vec<(f32, f32, f32)>, Colliders, u8))>;

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
            let at = (context.x, context.y);
            let circles = room_obstacles(context.seed, at)
                .1
                .iter()
                .map(|o| (o.x, o.y, o.r))
                .collect();
            let open = open_sides(context.seed, at);
            *cache = Some((key, (circles, room_ground(context.seed, at), open)));
        }
        let (obstacles, ground, open) = &cache.as_ref().expect("filled above").1;
        step_among(*state, *input, obstacles, ground, *open)
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

/// One command frame on flat floor with the classic pillars (no ramps).
pub fn step(s: PitState, input: PitInput) -> PitState {
    step_among(s, input, &arena::PILLARS, &Colliders::new(), 0)
}

/// How high a player steps up onto walkable ground without jumping (m),
/// over what it already stands on. A walkable slope rises less than this
/// in a frame; the side of a tall ramp does not.
pub const STEP_UP: f32 = 0.3;
/// Walking down a slope, ground this far below the feet still holds them
/// (m); past an edge they fall.
const SNAP_DOWN: f32 = 0.35;
/// Touching the ground: feet within this of it (m), above the wire step.
const TOUCH: f32 = 0.02;
/// How fast a player slides off a slope too steep to stand on (m/s).
pub const SLIDE_SPEED: f32 = 6.0;
/// Speed kept along the normal when bouncing off a steep slope.
pub const BOUNCE_RESTITUTION: f32 = 0.5;
/// Slower than this into a steep slope (m/s), a player slides instead.
const BOUNCE_MIN: f32 = 2.0;

/// Feet this low (m) have fallen into a hole: the step holds them here
/// (inside the wire range) and the room counts the player dead.
pub const FALL_FLOOR: f32 = -3.9;

/// Ground height and unit normal at `[x, y]`: the highest ramp, platform
/// or terrain there. Without terrain the floor is flat at 0; with it, no
/// ground is a hole ([`HOLE`]).
fn ground(ground: &Colliders, x: f32, y: f32) -> (f32, [f32; 3]) {
    let up = [0.0, 0.0, 1.0];
    match ground.ground_at([x, y], GROUND_MASK) {
        Some(g) => (g.height, g.normal),
        None if ground.terrain().is_some() => (HOLE, up),
        None => (0.0, up),
    }
}

/// Whether a capsule with feet at `z` may move to `[x, y]`: no point of
/// its footprint (centre and four rims) is on ground it can't climb —
/// walkable ground more than [`STEP_UP`] (plus the rim's own rise) above
/// the feet, or steep ground above them at all.
fn blocked(ramps: &Colliders, x: f32, y: f32, z: f32) -> bool {
    let r = arena::PLAYER_RADIUS;
    [(0.0, 0.0), (r, 0.0), (-r, 0.0), (0.0, r), (0.0, -r)]
        .into_iter()
        .any(|(dx, dy)| {
            let (g, n) = ground(ramps, x + dx, y + dy);
            if collide::walkable(n) {
                g > z + STEP_UP + if dx == 0.0 && dy == 0.0 { 0.0 } else { r }
            } else {
                g > z + TOUCH
            }
        })
}

/// Whether the move from `(x0, y0)` to `(x1, y1)` with feet at `z` hits
/// ground it can't climb anywhere along the way, not just at the end, so a
/// fast move (a dash plus a slide) can't tunnel through geometry between
/// one frame and the next. Thick ground (hills, ramps) is sampled along
/// the path at most a quarter of the player's width apart; thin solids
/// (platform edges) are swept with three horizontal rays (centre and both
/// rims) just above step-up height, reaching the leading rim — a hit
/// on a face too steep to stand on blocks.
fn swept_blocked(ramps: &Colliders, (x0, y0): (f32, f32), (x1, y1): (f32, f32), z: f32) -> bool {
    let (dx, dy) = (x1 - x0, y1 - y0);
    let dist = (dx * dx + dy * dy).sqrt();
    let steps = (dist / (arena::PLAYER_RADIUS / 2.0)).ceil().max(1.0) as u32;
    if (1..=steps).any(|i| {
        let t = i as f32 / steps as f32;
        blocked(ramps, x0 + dx * t, y0 + dy * t, z)
    }) {
        return true;
    }
    if dist <= 1e-6 {
        return false;
    }
    let (ux, uy) = (dx / dist, dy / dist);
    let r = arena::PLAYER_RADIUS;
    let height = z + STEP_UP + TOUCH;
    [0.0, r, -r].into_iter().any(|side| {
        let origin = [x0 - uy * side, y0 + ux * side, height];
        let ray = Ray::new(origin, [ux, uy, 0.0], dist + r).mask(LAYER_SOLID);
        ramps
            .raycast(&ray)
            .is_some_and(|hit| !collide::walkable(hit.normal))
    })
}

/// One command frame: [`arena::advance_among`] (stick, dash, the room's
/// obstacles), then the walls with their doorway passages, then the
/// ramps and the jump, snapped to the wire precision.
///
/// The ramps: a move onto ground the player can't climb is refused (it
/// slides along the other axis if that one is free). Ground counts as
/// floor only when [`collide::walkable`] (normal up component above 0.7,
/// about 45°): on it the feet follow the surface and the player can jump.
/// On a steeper slope it slides downhill at [`SLIDE_SPEED`]; landing on
/// one fast, its velocity is reflected about the slope's normal (keeping
/// [`BOUNCE_RESTITUTION`] of the part along it): it bounces off.
pub fn step_among(
    s: PitState,
    input: PitInput,
    obstacles: &[(f32, f32, f32)],
    ramps: &Colliders,
    open: u8,
) -> PitState {
    let planar = s.planar();
    let (g0, n0) = ground(ramps, s.x, s.y);
    let touching = s.z <= g0 + TOUCH && s.vz <= 0.0;
    let on_floor = touching && collide::walkable(n0);
    // Half the grip off the ground.
    let rates = if touching {
        RATES
    } else {
        arena::Rates {
            accel: RATES.accel * 0.5,
            decel: RATES.decel * 0.5,
            reverse: RATES.reverse * 0.5,
        }
    };
    let mut next = walls(
        planar,
        arena::advance_with(planar, input.movement(), obstacles, rates),
        open,
    );
    let from = (s.x, s.y);
    if (!ramps.is_empty() || ramps.terrain().is_some())
        && swept_blocked(ramps, from, (next.x, next.y), s.z)
    {
        if !swept_blocked(ramps, from, (next.x, s.y), s.z) {
            (next.y, next.vy) = (s.y, 0.0);
        } else if !swept_blocked(ramps, from, (s.x, next.y), s.z) {
            (next.x, next.vx) = (s.x, 0.0);
        } else {
            (next.x, next.y, next.vx, next.vy) = (s.x, s.y, 0.0, 0.0);
        }
    }
    let (mut z, mut vz) = (s.z, s.vz);
    if touching && !on_floor {
        // Too steep to stand on: slide down it.
        let (dx, dy) = (n0[0], n0[1]);
        let l = (dx * dx + dy * dy).sqrt();
        if l > 1e-6 {
            let k = SLIDE_SPEED * arena::DT / l;
            next = walls(
                planar,
                MoveState {
                    x: next.x + dx * k,
                    y: next.y + dy * k,
                    ..next
                },
                open,
            );
        }
        vz = 0.0;
    }
    let (g1, n1) = ground(ramps, next.x, next.y);
    // Coyote time and the jump buffer (whole ticks: the step is fixed at
    // DT). A press (the button's first tick) is wanted for JUMP_BUFFER
    // ticks; it fires on the first tick on the floor or within COYOTE
    // ticks of leaving it, and firing spends both (one press, one jump).
    // Ground too steep to stand on is no floor to jump from, then or after.
    let mut air = if on_floor {
        0
    } else if touching {
        JUMPED
    } else {
        s.air.saturating_add(1)
    };
    let pressed = input.jump() && !s.held;
    let mut buf = if pressed { JUMP_BUFFER } else { s.buf };
    if buf > 0 && air <= COYOTE {
        vz = JUMP_SPEED;
        buf = 0;
        air = JUMPED;
    } else {
        buf = buf.saturating_sub(1);
    }
    let held = input.jump();
    if touching && vz <= 0.0 && g1 >= z - SNAP_DOWN {
        // Walking (or sliding): the feet follow the ground.
        z = g1;
        vz = 0.0;
    } else {
        z += vz * arena::DT;
        vz -= GRAVITY * arena::DT;
        if z <= g1 {
            z = g1;
            if collide::walkable(n1) {
                vz = 0.0;
            } else {
                // A steep slope: bounce off it (or slide, if slow).
                let v = [next.vx, next.vy, vz];
                let along = v[0] * n1[0] + v[1] * n1[1] + v[2] * n1[2];
                let out = if along < -BOUNCE_MIN {
                    let r = collide::reflect(v, n1);
                    let k = (1.0 - BOUNCE_RESTITUTION) * along;
                    [r[0] + k * n1[0], r[1] + k * n1[1], r[2] + k * n1[2]]
                } else {
                    [v[0] - along * n1[0], v[1] - along * n1[1], 0.0]
                };
                (next.vx, next.vy, vz) = (out[0], out[1], out[2]);
            }
        }
        if z < FALL_FLOOR {
            // Down a hole: held at the bottom until the room respawns it.
            (z, vz) = (FALL_FLOOR, 0.0);
            (next.vx, next.vy) = (0.0, 0.0);
        }
    }
    canonicalize(
        PitState {
            z,
            vz,
            air,
            buf,
            held,
            ..s
        }
        .with_planar(next),
    )
}

/// Snap to the replicated precision of a Mob Pit player.
pub fn canonicalize(s: PitState) -> PitState {
    PitState {
        x: arena::snap(POS, s.x),
        y: arena::snap(POS, s.y),
        vx: arena::snap(arena::VEL, s.vx),
        vy: arena::snap(arena::VEL, s.vy),
        z: arena::snap(HEIGHT, s.z),
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

/// Keep `s` (one frame after `prev`) inside the room square or a passage:
/// a doorway's, or along the whole of an open side ([`open_sides`] bits
/// in `open`). A player already in a passage stays within it; one in the
/// room may enter a passage only through its gap. The corner squares
/// (past two sides at once) stay walled even where both sides are open.
fn walls(prev: MoveState, s: MoveState, open: u8) -> MoveState {
    let lim = arena::HALF - arena::PLAYER_RADIUS;
    // Snapped to the wire, a player held at the wall may sit a hair past it.
    let inside = lim + SLACK;
    let far = arena::HALF + PASSAGE;
    // Half width of the passage through the side facing `v` on that axis.
    let gap = |v: f32, (pos, neg): (u8, u8)| {
        let bit = if v > 0.0 { pos } else { neg };
        if open & bit != 0 {
            lim
        } else {
            DOOR_HALF - arena::PLAYER_RADIUS
        }
    };
    let (sides_x, sides_y) = ((1, 2), (4, 8));
    let mut s = s;
    let (lim_x, lim_y) = if prev.x.abs() > inside {
        (far, gap(prev.x, sides_x))
    } else if prev.y.abs() > inside {
        (gap(prev.y, sides_y), far)
    } else {
        (
            if s.y.abs() <= gap(s.x, sides_x) {
                far
            } else {
                lim
            },
            if s.x.abs() <= gap(s.y, sides_y) {
                far
            } else {
                lim
            },
        )
    };
    (s.x, s.vx) = clamp_axis(s.x, s.vx, lim_x);
    (s.y, s.vy) = clamp_axis(s.y, s.vy, lim_y);
    if s.x.abs() > inside && s.y.abs() > inside {
        // Into a corner through two open sides: keep the deeper one.
        if s.x.abs() >= s.y.abs() {
            (s.y, s.vy) = clamp_axis(s.y, s.vy, lim);
        } else {
            (s.x, s.vx) = clamp_axis(s.x, s.vx, lim);
        }
    }
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

/// [`RING8`], for traces and tests.
pub const RING8_DIRS: [(f32, f32); 8] = RING8;

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
    let footprints = room_ramps(seed, room)
        .iter()
        .map(|r| (r.x0, r.y0, r.x1, r.y1))
        .chain(
            room_platforms(seed, room)
                .iter()
                .map(|p| (p.x0, p.y0, p.x1, p.y1)),
        )
        .collect::<Vec<_>>();
    for (x0, y0, x1, y1) in footprints {
        let (hx, hy) = ((x1 - x0) / 2.0, (y1 - y0) / 2.0);
        keep.push((x0 + hx, y0 + hy, (hx * hx + hy * hy).sqrt() + GAP));
    }
    let holes = room_holes(seed, room);
    let mut placed: Vec<Obstacle> = Vec::new();
    let place = |x: f32, y: f32, r: f32, look: u8, placed: &mut Vec<Obstacle>| {
        if placed.len() < MAX_OBSTACLES
            && fits(x, y, r, &keep, placed)
            && clear_of_holes(x, y, r, &holes)
        {
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

/// A sloped ramp of a room (room-local): a wedge over an axis-aligned
/// rectangle whose top climbs from the floor at one edge to `height` at the
/// opposite one ([`collide::Collider::ramp`]).
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct Ramp {
    /// Lowest x (m, room-local).
    pub x0: f32,
    /// Lowest y.
    pub y0: f32,
    /// Highest x.
    pub x1: f32,
    /// Highest y.
    pub y1: f32,
    /// Height of the high edge (m).
    pub height: f32,
    /// Which way it climbs: 0 +x · 1 −x · 2 +y · 3 −y.
    pub uphill: u8,
    /// Slope (whole degrees, [`RAMP_DEGREES`]).
    pub degrees: u8,
}

impl Ramp {
    /// Its collider.
    pub fn collider(&self) -> Collider {
        let uphill = match self.uphill {
            0 => Uphill::PosX,
            1 => Uphill::NegX,
            2 => Uphill::PosY,
            _ => Uphill::NegY,
        };
        Collider::ramp([self.x0, self.y0], [self.x1, self.y1], self.height, uphill)
            .on_layers(LAYER_RAMP)
    }

    /// Whether a player can walk up it ([`collide::walkable`]).
    pub fn walkable(&self) -> bool {
        self.degrees <= 45
    }
}

/// The slopes a ramp may have (degrees, inclusive): some walkable, some
/// too steep to stand on (over about 45°).
pub const RAMP_DEGREES: (u8, u8) = (10, 60);
/// `tan` of 10°..=60°, as literals (no trig: native and WebAssembly agree
/// bit for bit).
const TAN: [f32; 51] = [
    0.17632698, 0.19438031, 0.21255656, 0.23086819, 0.249328, 0.2679492, 0.2867454, 0.30573067,
    0.3249197, 0.3443276, 0.36397022, 0.38386405, 0.40402624, 0.4244748, 0.4452287, 0.46630767,
    0.4877326, 0.5095255, 0.53170943, 0.55430907, 0.57735026, 0.6008606, 0.62486935, 0.64940757,
    0.6745085, 0.70020753, 0.72654253, 0.75355405, 0.78128564, 0.80978405, 0.83909965, 0.8692867,
    0.90040404, 0.9325151, 0.96568877, 1.0, 1.0355303, 1.0723687, 1.1106125, 1.1503685, 1.1917536,
    1.2348971, 1.2799417, 1.3270448, 1.3763819, 1.428148, 1.482561, 1.539865, 1.6003345, 1.6642795,
    1.7320508,
];
/// Most ramps a room has.
pub const MAX_RAMPS: usize = 3;
/// Hash salt of the room ramps.
const SALT_RAMP: u32 = 903;
/// Layer bits of the room geometry ([`room_colliders`]).
pub const LAYER_SOLID: u32 = layer::DEFAULT;
/// Ramps.
pub const LAYER_RAMP: u32 = 1 << 1;
/// The floor (lasers bounce off it; movers ignore it).
pub const LAYER_FLOOR: u32 = 1 << 2;
/// Holes, for mobs to keep out of (shots and players pass).
pub const LAYER_HOLE: u32 = 1 << 3;
/// The wall lines above the walls, for lasers only: a shot leaves a room
/// through a doorway or an open side, never over a wall.
pub const LAYER_BARRIER: u32 = 1 << 4;
/// What a laser meets: everything but the holes' footprints.
pub const LASER_MASK: u32 = !LAYER_HOLE;
/// What can be stood on: everything but the laser barrier.
pub const GROUND_MASK: u32 = !LAYER_BARRIER;
/// How high the laser barrier reaches (m): above any shot's whole range.
const BARRIER_HEIGHT: f32 = 1000.0;
/// Height of the room walls and obstacles a laser meets (m).
pub const WALL_HEIGHT: f32 = 1.6;
/// Obstacles stop shots below this height (m).
pub const OBSTACLE_HEIGHT: f32 = 2.4;
/// Wall thickness (m), just outside the floor.
pub const WALL_THICKNESS: f32 = 0.4;

/// The 1–[`MAX_RAMPS`] ramps of room `room`: random whole-degree slopes in
/// [`RAMP_DEGREES`], each clear (by its bounding circle) of the door
/// corridors, walls, spawn points, mob homes, the classic pillars (in a
/// classic room) and each other. Placed before the room's other
/// obstacles, which keep clear of them. Pure and deterministic.
pub fn room_ramps(seed: u32, room: (i32, i32)) -> Vec<Ramp> {
    let h = grid::mix(seed, SALT_RAMP, room.0, room.1);
    let pattern = if room == (0, 0) {
        0
    } else {
        grid::mix(seed, SALT_OBSTACLE, room.0, room.1) % PATTERNS.len() as u32
    };
    let mut keep: Vec<(f32, f32, f32)> = KEEP.to_vec();
    for m in mob_slots(seed, room) {
        keep.push((m.x, m.y, 0.8 + GAP));
    }
    let mut placed: Vec<Obstacle> = if pattern == 0 {
        arena::PILLARS
            .iter()
            .map(|&(x, y, r)| Obstacle { x, y, r, look: 0 })
            .collect()
    } else {
        Vec::new()
    };
    let holes = room_holes(seed, room);
    let count = 1 + (h % MAX_RAMPS as u32) as usize;
    let roll = |i: u32| grid::unit(grid::hash32(h ^ i.wrapping_mul(0x9e37_79b9)));
    let mut ramps = Vec::new();
    for i in 0..256u32 {
        if ramps.len() == count {
            break;
        }
        let k = 8 * i;
        let span = u32::from(RAMP_DEGREES.1 - RAMP_DEGREES.0) + 1;
        let degrees = RAMP_DEGREES.0 + (grid::hash32(h ^ (k + 1)) % span) as u8;
        let tan = TAN[usize::from(degrees - RAMP_DEGREES.0)];
        let uphill = (grid::hash32(h ^ (k + 2)) % 4) as u8;
        let mut height = 0.8 + 1.4 * roll(k + 3);
        let mut run = height / tan;
        if run > 9.0 {
            run = 9.0;
            height = run * tan;
        }
        let across = 2.5 + 1.5 * roll(k + 4);
        let (hx, hy) = if uphill < 2 {
            (run / 2.0, across / 2.0)
        } else {
            (across / 2.0, run / 2.0)
        };
        let x = -14.0 + 28.0 * roll(k + 5);
        let y = -14.0 + 28.0 * roll(k + 6);
        let r = (hx * hx + hy * hy).sqrt();
        if !fits(x, y, r, &keep, &placed)
            || !clear_of_holes(x, y, r, &holes)
            || x.abs() < CROSS + hx
            || y.abs() < CROSS + hy
        {
            continue;
        }
        placed.push(Obstacle { x, y, r, look: 0 });
        ramps.push(Ramp {
            x0: x - hx,
            y0: y - hy,
            x1: x + hx,
            y1: y + hy,
            height,
            uphill,
            degrees,
        });
    }
    ramps
}

/// A hole of a room (room-local): a trench across it with no floor. A
/// player who falls in dies.
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct Hole {
    /// Lowest x (m, room-local).
    pub x0: f32,
    /// Lowest y.
    pub y0: f32,
    /// Highest x.
    pub x1: f32,
    /// Highest y.
    pub y1: f32,
}

impl Hole {
    /// Whether `[x, y]` is over it.
    pub fn contains(&self, x: f32, y: f32) -> bool {
        x >= self.x0 && x <= self.x1 && y >= self.y0 && y <= self.y1
    }
}

/// A platform of a room (room-local): a box to jump onto, or (`height`
/// 0) a stepping stone across a hole.
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct Platform {
    /// Lowest x (m, room-local).
    pub x0: f32,
    /// Lowest y.
    pub y0: f32,
    /// Highest x.
    pub x1: f32,
    /// Highest y.
    pub y1: f32,
    /// Top (m).
    pub height: f32,
}

impl Platform {
    /// Its collider.
    pub fn collider(&self) -> Collider {
        Collider::aabb([self.x0, self.y0], [self.x1, self.y1], self.height).on_layers(LAYER_SOLID)
    }
}

/// A rolling hill: a smooth bump `amp · (1 − d²/r²)²` within `r`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Hill {
    x: f32,
    y: f32,
    r: f32,
    amp: f32,
}

/// Hash salts of the holes, platforms and hills.
const SALT_HOLE: u32 = 904;
const SALT_PLATFORM: u32 = 905;
const SALT_HILL: u32 = 906;
/// How wide a trench is (m): a running jump clears about 3 m, a jump-dash
/// 4 m and more. Edges sit on the terrain grid, so the hole is exact.
pub const HOLE_WIDTH: (f32, f32) = (2.0, 4.0);
/// Highest the terrain rises (m): with a jump on top, inside the wire's
/// height range.
pub const TERRAIN_TOP: f32 = 2.2;
/// Terrain sample spacing (m).
pub const TERRAIN_CELL: f32 = 0.5;

/// Whether a circle keeps [`GAP`] clear of every hole.
fn clear_of_holes(x: f32, y: f32, r: f32, holes: &[Hole]) -> bool {
    holes.iter().all(|h| {
        let (cx, cy) = (x.clamp(h.x0, h.x1), y.clamp(h.y0, h.y1));
        let (dx, dy) = (x - cx, y - cy);
        dx * dx + dy * dy >= (r + GAP) * (r + GAP) && !h.contains(x, y)
    })
}

/// Half width of the clear cross along the door axes (m): ramps and
/// platforms keep off it, so each doorway has a straight walk to the others
/// (holes may still cut it).
const CROSS: f32 = 2.6;

/// Whether a circle keeps off the door-axis cross.
fn clear_of_cross(x: f32, y: f32, r: f32) -> bool {
    x.abs() >= CROSS + r && y.abs() >= CROSS + r
}

/// The holes of room `room`: in about three rooms in ten (never the
/// entrance or its neighbours), one trench — sometimes two — across the room, 2–4.2 m
/// wide, far enough out to keep clear of the spawn points and the boss's
/// home. Crossing the room means jumping it. Pure and deterministic.
pub fn room_holes(seed: u32, room: (i32, i32)) -> Vec<Hole> {
    if room.0.unsigned_abs() + room.1.unsigned_abs() < 2 {
        return Vec::new();
    }
    let h = grid::mix(seed, SALT_HOLE, room.0, room.1);
    if h % 10 >= 3 {
        return Vec::new();
    }
    let roll = |i: u32| grid::unit(grid::hash32(h ^ i.wrapping_mul(0x9e37_79b9)));
    let trenches = if h & 0x100 != 0 { 2 } else { 1 };
    let mut holes = Vec::new();
    for t in 0..trenches {
        let k = 4 * t;
        let w = HOLE_WIDTH.0 + (HOLE_WIDTH.1 - HOLE_WIDTH.0) * roll(k + 1);
        let c = 10.5 + w / 2.0 + (7.0 - w) * roll(k + 2);
        // The second trench is on the other side of the same axis.
        let sign = if (h >> 9) & 1 == t { 1.0 } else { -1.0 };
        let along_x = (h >> 10) & 1 == 0;
        // On the terrain grid: the cells inside are exactly the trench.
        let snap = |v: f32| (v / TERRAIN_CELL).round() * TERRAIN_CELL;
        let (lo, hi) = (snap(sign * c - w / 2.0), snap(sign * c + w / 2.0));
        let half = arena::HALF;
        holes.push(if along_x {
            Hole {
                x0: -half,
                y0: lo,
                x1: half,
                y1: hi,
            }
        } else {
            Hole {
                x0: lo,
                y0: -half,
                x1: hi,
                y1: half,
            }
        });
    }
    holes
}

/// Seed salt of the shared edges ([`open_sides`]).
const SALT_EDGE: u32 = 907;
/// Chance an edge is open (percent).
const OPEN_PERCENT: u32 = 35;

/// Whether the edge between `room` and its neighbour at offset `step` is
/// open (no wall: the whole side is a passage). Seeded from the edge's
/// own coordinates (twice its midpoint), so both rooms agree. Never next
/// to the entrance, and never where a trench of either room ends on it
/// (that would be a way round the trench).
fn edge_open(seed: u32, room: (i32, i32), step: (i32, i32)) -> bool {
    let other = (room.0 + step.0, room.1 + step.1);
    let near_entrance = |r: (i32, i32)| r.0.unsigned_abs() + r.1.unsigned_abs() < 2;
    let trench_ends = |r: (i32, i32), along_x: bool| {
        room_holes(seed, r).iter().any(|h| {
            if along_x {
                h.x1 - h.x0 >= ROOM_SIZE - SLACK
            } else {
                h.y1 - h.y0 >= ROOM_SIZE - SLACK
            }
        })
    };
    let (ex, ey) = (2 * room.0 + step.0, 2 * room.1 + step.1);
    !near_entrance(room)
        && !near_entrance(other)
        && grid::mix(seed, SALT_EDGE, ex, ey) % 100 < OPEN_PERCENT
        && !trench_ends(room, step.0 != 0)
        && !trench_ends(other, step.0 != 0)
}

/// The open sides of room `room`, as [`DOORWAYS`] bits.
pub fn open_sides(seed: u32, room: (i32, i32)) -> u8 {
    DOORWAYS
        .iter()
        .filter(|&&(_, step)| edge_open(seed, room, step))
        .fold(0, |bits, &(bit, _)| bits | bit)
}

/// The platforms of room `room`: 1–3 boxes 0.6–1.3 m high (a jump up),
/// clear like the ramps; and in a trench wider than 3 m, one or two
/// stepping stones at floor level to hop across on. Pure and
/// deterministic.
pub fn room_platforms(seed: u32, room: (i32, i32)) -> Vec<Platform> {
    let h = grid::mix(seed, SALT_PLATFORM, room.0, room.1);
    let holes = room_holes(seed, room);
    let roll = |i: u32| grid::unit(grid::hash32(h ^ i.wrapping_mul(0x9e37_79b9)));
    let mut platforms = Vec::new();
    for (n, hole) in holes.iter().enumerate() {
        let (w, along_x) = if hole.x1 - hole.x0 > hole.y1 - hole.y0 {
            (hole.y1 - hole.y0, true)
        } else {
            (hole.x1 - hole.x0, false)
        };
        if w <= 3.0 {
            continue;
        }
        let stones = 1 + (roll(50 + n as u32) * 2.0) as u32;
        for k in 0..stones {
            let t = -14.0 + 28.0 * roll(60 + 4 * n as u32 + k);
            let (cx, cy) = if along_x {
                (t, (hole.y0 + hole.y1) / 2.0)
            } else {
                ((hole.x0 + hole.x1) / 2.0, t)
            };
            platforms.push(Platform {
                x0: cx - 0.6,
                y0: cy - 0.6,
                x1: cx + 0.6,
                y1: cy + 0.6,
                height: 0.0,
            });
        }
    }
    let mut keep: Vec<(f32, f32, f32)> = KEEP.to_vec();
    for m in mob_slots(seed, room) {
        keep.push((m.x, m.y, 0.8 + GAP));
    }
    let pattern = if room == (0, 0) {
        0
    } else {
        grid::mix(seed, SALT_OBSTACLE, room.0, room.1) % PATTERNS.len() as u32
    };
    let mut placed: Vec<Obstacle> = if pattern == 0 {
        arena::PILLARS
            .iter()
            .map(|&(x, y, r)| Obstacle { x, y, r, look: 0 })
            .collect()
    } else {
        Vec::new()
    };
    for r in room_ramps(seed, room) {
        let (hx, hy) = ((r.x1 - r.x0) / 2.0, (r.y1 - r.y0) / 2.0);
        placed.push(Obstacle {
            x: r.x0 + hx,
            y: r.y0 + hy,
            r: (hx * hx + hy * hy).sqrt(),
            look: 0,
        });
    }
    let count = 1 + (h % 3) as usize;
    let mut boxes = 0;
    for i in 0..48u32 {
        if boxes == count {
            break;
        }
        let k = 8 * i;
        let (hx, hy) = (0.8 + 0.8 * roll(k + 1), 0.8 + 0.8 * roll(k + 2));
        let height = 0.6 + 0.7 * roll(k + 3);
        let x = -14.0 + 28.0 * roll(k + 4);
        let y = -14.0 + 28.0 * roll(k + 5);
        let r = (hx * hx + hy * hy).sqrt();
        if !fits(x, y, r, &keep, &placed)
            || !clear_of_holes(x, y, r, &holes)
            || !clear_of_cross(x, y, r)
        {
            continue;
        }
        placed.push(Obstacle { x, y, r, look: 0 });
        platforms.push(Platform {
            x0: x - hx,
            y0: y - hy,
            x1: x + hx,
            y1: y + hy,
            height,
        });
        boxes += 1;
    }
    platforms
}

/// The rolling hills of room `room`: none in the entrance; in seven rooms
/// in ten, two to five bumps 0.5–1.8 m high and 4–9 m across.
fn room_hills(seed: u32, room: (i32, i32)) -> Vec<Hill> {
    if room == (0, 0) {
        return Vec::new();
    }
    let h = grid::mix(seed, SALT_HILL, room.0, room.1);
    if h % 10 >= 7 {
        return Vec::new();
    }
    let roll = |i: u32| grid::unit(grid::hash32(h ^ i.wrapping_mul(0x9e37_79b9)));
    let count = 2 + (grid::hash32(h ^ 0x51) % 4);
    (0..count)
        .map(|i| Hill {
            x: -15.0 + 30.0 * roll(4 * i + 1),
            y: -15.0 + 30.0 * roll(4 * i + 2),
            r: 4.0 + 5.0 * roll(4 * i + 3),
            // At most 0.35 r high: the steepest flank stays walkable.
            amp: (0.5 + 1.3 * roll(4 * i + 4)).min(0.35 * (4.0 + 5.0 * roll(4 * i + 3))),
        })
        .collect()
}

/// `t` eased from 0 to 1 over `0..=1` (smoothstep).
fn ease(t: f32) -> f32 {
    let t = t.clamp(0.0, 1.0);
    t * t * (3.0 - 2.0 * t)
}

/// The hills of room `room` (summed), eased to level 2 m round each ramp
/// and platform (which stand on the floor): its terrain before the walls,
/// holes and obstacles.
fn hill_height(hills: &[Hill], flat: &[(f32, f32, f32)], x: f32, y: f32) -> f32 {
    let mut z = 0.0f32;
    for hill in hills {
        let (dx, dy) = (x - hill.x, y - hill.y);
        let u = (dx * dx + dy * dy) / (hill.r * hill.r);
        if u < 1.0 {
            z += hill.amp * (1.0 - u) * (1.0 - u);
        }
    }
    let mut fade = 1.0;
    for &(fx, fy, fr) in flat {
        let (dx, dy) = (x - fx, y - fy);
        fade *= ease(((dx * dx + dy * dy).sqrt() - fr) / 2.0);
    }
    (z * fade).min(TERRAIN_TOP)
}

/// Eased to level by 0.5 m from the walls: rooms meet flat.
fn wall_fade(x: f32, y: f32) -> f32 {
    ease((arena::HALF - x.abs().max(y.abs()) - 0.5) / 3.5)
}

/// Room `room`'s ramps and platforms as circles `(x, y, r)` the hills
/// ease to level round.
fn ramp_and_platform_circles(seed: u32, room: (i32, i32)) -> Vec<(f32, f32, f32)> {
    room_ramps(seed, room)
        .iter()
        .map(|r| (r.x0, r.y0, r.x1, r.y1))
        .chain(
            room_platforms(seed, room)
                .iter()
                .map(|p| (p.x0, p.y0, p.x1, p.y1)),
        )
        .map(|(x0, y0, x1, y1)| {
            let (hx, hy) = ((x1 - x0) / 2.0, (y1 - y0) / 2.0);
            (x0 + hx, y0 + hy, (hx * hx + hy * hy).sqrt())
        })
        .collect()
}

/// The ground level each of room `room`'s obstacles stands on (in
/// [`room_obstacles`] order): the hill under its centre. The terrain is
/// level at that height out to 1 m past it (sloping with the walls' fade
/// near a wall), so it stands on a hill at its full height, never sunk in
/// one (nor left a stub that looks like a platform).
pub fn obstacle_levels(seed: u32, room: (i32, i32)) -> Vec<f32> {
    let hills = room_hills(seed, room);
    let flat = ramp_and_platform_circles(seed, room);
    let obstacles = room_obstacles(seed, room).1;
    let mut levels: Vec<f32> = obstacles
        .iter()
        .map(|o| {
            if hills.is_empty() {
                0.0
            } else {
                hill_height(&hills, &flat, o.x, o.y) * wall_fade(o.x, o.y)
            }
        })
        .collect();
    // Obstacles whose patches (or the 2 m eases round them) meet share one
    // level, the highest: each stands level, none sunk.
    let n = obstacles.len();
    let mut group: Vec<usize> = (0..n).collect();
    let root = |group: &[usize], mut i: usize| {
        while group[i] != i {
            i = group[i];
        }
        i
    };
    for i in 0..n {
        for j in (i + 1)..n {
            let (a, b) = (&obstacles[i], &obstacles[j]);
            let reach = a.r + b.r + 2.0 + 2.0 * 2.0;
            if (a.x - b.x).powi(2) + (a.y - b.y).powi(2) < reach * reach {
                let (ri, rj) = (root(&group, i), root(&group, j));
                group[ri.max(rj)] = ri.min(rj);
            }
        }
    }
    let mut top = vec![f32::MIN; n];
    for (i, &level) in levels.iter().enumerate() {
        let r = root(&group, i);
        top[r] = top[r].max(level);
    }
    for (i, level) in levels.iter_mut().enumerate() {
        *level = top[root(&group, i)];
    }
    levels
}

/// Room `room`'s terrain over the floor and its passages: the hills
/// ([`hill_height`]), level round each obstacle at its
/// [`obstacle_levels`] height, eased to level near the walls, and
/// [`HOLE`] over the holes. Polynomials
/// only, so the browser's WebAssembly computes the same samples. Pure and
/// deterministic.
pub fn room_terrain(seed: u32, room: (i32, i32)) -> Heightfield {
    let hills = room_hills(seed, room);
    let holes = room_holes(seed, room);
    let flat = ramp_and_platform_circles(seed, room);
    let levels: Vec<(f32, f32, f32, f32)> = room_obstacles(seed, room)
        .1
        .iter()
        .zip(obstacle_levels(seed, room))
        .map(|(o, level)| (o.x, o.y, o.r + 1.0, level))
        .collect();
    let edge = arena::HALF + PASSAGE;
    let n = (2.0 * edge / TERRAIN_CELL) as usize + 1;
    Heightfield::new([-edge, -edge], TERRAIN_CELL, n, n, |x, y| {
        // Strictly inside: the edge samples are ground, so a cell is open
        // exactly when it lies in the trench.
        if holes
            .iter()
            .any(|h| x > h.x0 && x < h.x1 && y > h.y0 && y < h.y1)
        {
            return HOLE;
        }
        if hills.is_empty() {
            return 0.0;
        }
        let z = hill_height(&hills, &flat, x, y);
        // Toward the level of the obstacle that pulls hardest here (each
        // patch keeps its own level where two meet).
        let mut pull = (1.0f32, 0.0f32);
        for &(ox, oy, r, level) in &levels {
            let (dx, dy) = (x - ox, y - oy);
            let w = ease(((dx * dx + dy * dy).sqrt() - r) / 2.0);
            if w < pull.0 {
                pull = (w, level);
            }
        }
        let (w, level) = pull;
        let z = level + (z * wall_fade(x, y) - level) * w;
        // Rooms meet flat, even past an obstacle's patch near a wall.
        let wall = arena::HALF - x.abs().max(y.abs());
        z * ease((wall - 0.5) / 1.0)
    })
}

/// What a player stands on in room `room`: its ramps and platforms over
/// its terrain (the predicted step's ground).
pub fn room_ground(seed: u32, room: (i32, i32)) -> Colliders {
    room_platforms(seed, room)
        .iter()
        .map(Platform::collider)
        .fold(ramp_colliders(&room_ramps(seed, room)), Colliders::with)
        .with_terrain(room_terrain(seed, room), LAYER_FLOOR)
}

/// Room `room`'s terrain samples, for the page to draw ([`room_terrain`]).
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct RoomTerrain {
    /// First sample x (m, room-local).
    pub min_x: f32,
    /// First sample y.
    pub min_y: f32,
    /// Sample spacing (m).
    pub cell: f32,
    /// Samples along x.
    pub nx: u16,
    /// Samples along y.
    pub ny: u16,
    /// Heights row by row (`y` outer); [`TERRAIN_HOLE`] over a hole.
    pub heights: Vec<f32>,
}

/// [`RoomTerrain`] height of a hole sample.
pub const TERRAIN_HOLE: f32 = -1000.0;

/// Room `(x, y)`'s terrain (the page draws the ground with it and stands
/// mobs on it).
#[game_pure(RoomTerrainFn)]
pub fn terrain(room: &PitRoomContext) -> RoomTerrain {
    let field = room_terrain(room.seed, (room.x, room.y));
    let (nx, ny) = field.size();
    RoomTerrain {
        min_x: field.min()[0],
        min_y: field.min()[1],
        cell: field.cell(),
        nx: nx as u16,
        ny: ny as u16,
        heights: field
            .samples()
            .iter()
            .map(|&h| if h == HOLE { TERRAIN_HOLE } else { h })
            .collect(),
    }
}

/// A patch of floor around a point ([`floor_patch`]'s input): the polar
/// grid a mob's slam warning is draped on.
#[derive(Debug, Clone, Copy, Default, PartialEq, GameRecord)]
pub struct FloorPatch {
    /// World seed.
    pub seed: u32,
    /// Room x.
    pub room_x: i32,
    /// Room y.
    pub room_y: i32,
    /// Centre (room-local).
    pub x: f32,
    /// Centre y.
    pub y: f32,
    /// Out to this far (m).
    pub radius: f32,
    /// Rings of points out to `radius`.
    pub rings: u8,
    /// Points per ring (a power of two, 4 to 64; others round up).
    pub segments: u8,
}

/// `(seed, x, y)` → that room's ground ([`room_ground`]).
type GroundCache = Vec<((u32, i32, i32), Colliders)>;

thread_local! {
    /// Grounds of the rooms drawn lately (a few rooms are in view).
    static GROUNDS: RefCell<GroundCache> = const { RefCell::new(Vec::new()) };
}

/// Rooms whose ground [`floor_patch`] keeps.
const GROUNDS_KEPT: usize = 9;

/// The floor around a point, as the room judges a slam (`floor_at`: the
/// terrain, a ramp or a platform top, whichever is under it): the centre's
/// height, then ring by ring (`radius × r / rings`, r = 1..=rings) each
/// point at angle `2π s / segments` from +x. [`TERRAIN_HOLE`] over a hole.
/// The directions come from [`ring_dirs`] (no `sin`/`cos`: the page's
/// WASM copy must match the native one bit for bit).
#[game_pure(FloorPatchFn)]
pub fn floor_patch(patch: &FloorPatch) -> Vec<f32> {
    let key = (patch.seed, patch.room_x, patch.room_y);
    GROUNDS.with(|grounds| {
        let mut grounds = grounds.borrow_mut();
        let i = match grounds.iter().position(|(k, _)| *k == key) {
            Some(i) => i,
            None => {
                if grounds.len() >= GROUNDS_KEPT {
                    grounds.remove(0);
                }
                grounds.push((key, room_ground(key.0, (key.1, key.2))));
                grounds.len() - 1
            }
        };
        let ground = &grounds[i].1;
        let height = |x: f32, y: f32| {
            ground
                .ground_at([x, y], GROUND_MASK)
                .map_or(TERRAIN_HOLE, |g| g.height)
        };
        let rings = usize::from(patch.rings.clamp(1, 16));
        let dirs = ring_dirs(usize::from(patch.segments));
        let mut heights = Vec::with_capacity(1 + rings * dirs.len());
        heights.push(height(patch.x, patch.y));
        for r in 1..=rings {
            let d = patch.radius * r as f32 / rings as f32;
            for &(cx, cy) in &dirs {
                heights.push(height(patch.x + d * cx, patch.y + d * cy));
            }
        }
        heights
    })
}

/// `n` unit directions evenly round the circle from +x (`n` rounded up to
/// a power of two, 4 to 64), in plain arithmetic: the step angle by
/// half-angle square roots from a quarter turn, then turned by complex
/// multiplication. Bit-identical on every target, unlike `sin`/`cos`.
pub fn ring_dirs(n: usize) -> Vec<(f32, f32)> {
    let n = n.clamp(4, 64).next_power_of_two();
    let (mut c, mut s) = (0.0f32, 1.0f32);
    let mut quarter = 4;
    while quarter < n {
        (c, s) = (((1.0 + c) * 0.5).sqrt(), ((1.0 - c) * 0.5).sqrt());
        quarter *= 2;
    }
    let mut dirs = Vec::with_capacity(n);
    let (mut x, mut y) = (1.0f32, 0.0f32);
    for _ in 0..n {
        dirs.push((x, y));
        (x, y) = (x * c - y * s, x * s + y * c);
    }
    dirs
}

/// The ramps as colliders (what the predicted step stands on).
pub fn ramp_colliders(ramps: &[Ramp]) -> Colliders {
    ramps
        .iter()
        .map(Ramp::collider)
        .fold(Colliders::new(), Colliders::with)
}

/// Room `room`'s level geometry (room-local): the obstacles
/// ([`OBSTACLE_HEIGHT`]), platforms and walls with their doorway gaps
/// ([`WALL_HEIGHT`]) on [`LAYER_SOLID`], the ramps on [`LAYER_RAMP`], the
/// terrain (floor and passages, hills, holes) on [`LAYER_FLOOR`], and the
/// holes' footprints on [`LAYER_HOLE`] (mobs keep out; shots use
/// [`LASER_MASK`]), and above each wall its laser barrier on
/// [`LAYER_BARRIER`] (standing uses [`GROUND_MASK`]).
pub fn room_colliders(seed: u32, room: (i32, i32)) -> Colliders {
    let mut colliders = room_obstacles(seed, room)
        .1
        .iter()
        .zip(obstacle_levels(seed, room))
        .map(|(o, level)| {
            // From the floor up to its full height above its ground.
            Collider::cylinder([o.x, o.y], o.r, level + OBSTACLE_HEIGHT).on_layers(LAYER_SOLID)
        })
        .fold(room_ground(seed, room), Colliders::with);
    for hole in room_holes(seed, room) {
        colliders.push(
            Collider::aabb([hole.x0, hole.y0], [hole.x1, hole.y1], 0.0).on_layers(LAYER_HOLE),
        );
    }
    let (h, t, d) = (arena::HALF, WALL_THICKNESS, DOOR_HALF);
    let open = open_sides(seed, room);
    for sign in [-1.0f32, 1.0] {
        let (near, far) = (sign * h, sign * (h + t));
        let (north_south, east_west) = if sign > 0.0 { (4, 1) } else { (8, 2) };
        for (a, b) in [([-h - t, near], [-d, far]), ([d, near], [h + t, far])] {
            // A wall along x (north/south), then the same along y (east/west).
            // Each wall has its laser barrier on the same footprint.
            if open & north_south == 0 {
                colliders.push(Collider::aabb(a, b, WALL_HEIGHT).on_layers(LAYER_SOLID));
                colliders.push(Collider::aabb(a, b, BARRIER_HEIGHT).on_layers(LAYER_BARRIER));
            }
            if open & east_west == 0 {
                let (a, b) = ([a[1], a[0]], [b[1], b[0]]);
                colliders.push(Collider::aabb(a, b, WALL_HEIGHT).on_layers(LAYER_SOLID));
                colliders.push(Collider::aabb(a, b, BARRIER_HEIGHT).on_layers(LAYER_BARRIER));
            }
        }
        // Corner posts stand even between open sides.
        for other in [-1.0f32, 1.0] {
            let (x0, x1) = (sign * h, sign * (h + t));
            let (y0, y1) = (other * h, other * (h + t));
            colliders.push(
                Collider::aabb(
                    [x0.min(x1), y0.min(y1)],
                    [x0.max(x1), y0.max(y1)],
                    WALL_HEIGHT,
                )
                .on_layers(LAYER_SOLID),
            );
        }
    }
    colliders
}

/// Total path a laser shot travels (m), bounces included.
pub const LASER_RANGE: f32 = 100.0;
/// Most times a laser shot bounces before it stops.
pub const LASER_BOUNCES: usize = 4;

/// A laser shot through a room's static geometry ([`laser_path`]'s input).
#[derive(Debug, Clone, Copy, Default, PartialEq, GameRecord)]
pub struct LaserShot {
    /// World seed.
    pub seed: u32,
    /// Room x.
    pub room_x: i32,
    /// Room y.
    pub room_y: i32,
    /// Muzzle (room-local).
    pub x: f32,
    /// Muzzle y.
    pub y: f32,
    /// Muzzle height.
    pub z: f32,
    /// Aim (need not be unit).
    pub dx: f32,
    /// Aim y.
    pub dy: f32,
    /// Aim z.
    pub dz: f32,
}

/// One surface a laser met.
#[derive(Debug, Clone, Copy, Default, PartialEq, GameRecord)]
pub struct LaserHit {
    /// Where (room-local).
    pub x: f32,
    /// Where y.
    pub y: f32,
    /// Where z.
    pub z: f32,
    /// Surface normal there.
    pub nx: f32,
    /// Normal y.
    pub ny: f32,
    /// Normal z.
    pub nz: f32,
}

/// Where a laser goes through a room's walls, obstacles, ramps and floor
/// with no one in the way: each bounce, then where it stops (normal zero).
/// The page draws its shot with this before the room's verdict.
#[game_pure(LaserPathFn)]
pub fn laser_path(shot: &LaserShot) -> Vec<LaserHit> {
    let colliders = room_colliders(shot.seed, (shot.room_x, shot.room_y));
    let ray = Ray::new(
        [shot.x, shot.y, shot.z],
        [shot.dx, shot.dy, shot.dz],
        LASER_RANGE,
    )
    .mask(LASER_MASK);
    let mut trace = collide::bounce(ray, LASER_BOUNCES, |r| colliders.raycast(r));
    trace.stop_at(&colliders, LAYER_BARRIER);
    let mut path: Vec<LaserHit> = trace
        .hits
        .iter()
        .map(|h| LaserHit {
            x: h.point[0],
            y: h.point[1],
            z: h.point[2],
            nx: h.normal[0],
            ny: h.normal[1],
            nz: h.normal[2],
        })
        .collect();
    // Where it stopped, unless that was its last surface (out of bounces,
    // or a wall barrier).
    let stopped_on_hit = trace.hits.last().is_some_and(|h| h.point == trace.end);
    if !stopped_on_hit {
        path.push(LaserHit {
            x: trace.end[0],
            y: trace.end[1],
            z: trace.end[2],
            ..LaserHit::default()
        });
    }
    path
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
    /// Its sloped ramps ([`room_ramps`]).
    pub ramps: Vec<Ramp>,
    /// Its holes ([`room_holes`]): fall in and die.
    pub holes: Vec<Hole>,
    /// Its platforms ([`room_platforms`]).
    pub platforms: Vec<Platform>,
    /// Open sides ([`open_sides`]: [`DOORWAYS`] bits): no wall, the whole
    /// side is a passage.
    pub open: u8,
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
    let holes = room_holes(room.seed, at);
    // Mob homes in a trench move to its nearer edge.
    for m in &mut mobs {
        for hole in &holes {
            let margin = 1.4;
            let grown = Hole {
                x0: hole.x0 - margin,
                y0: hole.y0 - margin,
                x1: hole.x1 + margin,
                y1: hole.y1 + margin,
            };
            if !grown.contains(m.x, m.y) {
                continue;
            }
            // Toward the room's centre if it was on that side, else out.
            let (v, lo, hi) = if hole.x1 - hole.x0 > hole.y1 - hole.y0 {
                (&mut m.y, grown.y0, grown.y1)
            } else {
                (&mut m.x, grown.x0, grown.x1)
            };
            let mid = (lo + hi) / 2.0;
            let lim = arena::HALF - 1.0;
            *v = if v.abs() < mid.abs() {
                if mid > 0.0 {
                    lo
                } else {
                    hi
                }
            } else if mid > 0.0 {
                hi.min(lim)
            } else {
                lo.max(-lim)
            };
        }
    }
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
        ramps: room_ramps(room.seed, at),
        holes,
        platforms: room_platforms(room.seed, at),
        open: open_sides(room.seed, at),
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
    /// Proximity wake: a doorway neighbour wakes within this distance of
    /// its doorway ([`WAKE_REACH`], m).
    pub wake_reach: f32,
    /// … and is released beyond this one ([`WAKE_RELEASE`], m).
    pub wake_release: f32,
    /// Room wall height (m): what a laser bounces off.
    pub wall_height: f32,
    /// Wall thickness (m), just outside the floor.
    pub wall_thickness: f32,
    /// Obstacle height a shot meets (m).
    pub obstacle_height: f32,
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
        wake_reach: WAKE_REACH,
        wake_release: WAKE_RELEASE,
        wall_height: WALL_HEIGHT,
        wall_thickness: WALL_THICKNESS,
        obstacle_height: OBSTACLE_HEIGHT,
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

// ------------------------------------------------------------ proximity wake

/// A doorway neighbour wakes (its watcher becomes *near*) when the player
/// is this close to the doorway leading to it (m, room-local, measured to
/// the middle of the doorway on the shared wall). Below half the room
/// (20 m) so that at most two doorways — the two of a corner — are ever in
/// reach, and nothing is from the middle of the room.
pub const WAKE_REACH: f32 = 11.0;
/// A near neighbour goes back to a passive watcher only beyond this
/// distance from its doorway ([`WAKE_REACH`] + 8 m hysteresis). Below 20 m,
/// so the middle of the room releases every neighbour.
pub const WAKE_RELEASE: f32 = WAKE_REACH + 8.0;
/// At most this many neighbours near at once (two doorways of a corner).
pub const WAKE_MAX: usize = 2;
/// An observer ticket grants `wake` only for rooms at most this many
/// doorways from the room the principal was last seen in (one doorway of
/// slack: a watcher for the next room's neighbours is asked for right after
/// a doorway, before the presence record follows).
pub const WAKE_DOORS: u32 = 2;

/// The doorway neighbours as [`Reach::near`] bits and room offsets: east
/// (+x), west, north (+y), south. Corner (diagonal) rooms have no doorway
/// to this one and never wake.
pub const DOORWAYS: [(u8, (i32, i32)); 4] = [(1, (1, 0)), (2, (-1, 0)), (4, (0, 1)), (8, (0, -1))];

/// Where the player stands (room-local) and which doorway neighbours were
/// near ([`rooms_in_reach`]'s input).
#[derive(Debug, Clone, Copy, Default, PartialEq, GameRecord)]
pub struct ReachContext {
    /// Room-local x (m).
    pub x: f32,
    /// Room-local y (m).
    pub y: f32,
    /// [`DOORWAYS`] bits near so far (the hysteresis).
    pub near: u8,
    /// The room's open sides ([`PitRoomLayout::open`]): reach is measured
    /// to the whole side, not its doorway.
    pub open: u8,
}

/// The doorway neighbours near now.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, GameRecord)]
pub struct Reach {
    /// [`DOORWAYS`] bits.
    pub near: u8,
}

/// Proximity wake: which of the four doorway neighbours a player at
/// room-local `(x, y)` keeps awake. A neighbour becomes near within
/// [`WAKE_REACH`] of its doorway (of the shared side, if it is open) and stays near until beyond
/// [`WAKE_RELEASE`]; at most [`WAKE_MAX`] (the nearest). Pure, so both
/// pages run this code as WebAssembly.
#[game_pure(RoomsInReachFn)]
pub fn rooms_in_reach(at: &ReachContext) -> Reach {
    let mut near: Vec<(f32, u8)> = DOORWAYS
        .iter()
        .filter_map(|&(bit, (dx, dy))| {
            let d = if at.open & bit != 0 {
                // Open side: the distance to its line.
                arena::HALF - (dx as f32 * at.x + dy as f32 * at.y)
            } else {
                let door = (dx as f32 * arena::HALF, dy as f32 * arena::HALF);
                ((at.x - door.0).powi(2) + (at.y - door.1).powi(2)).sqrt()
            };
            let limit = if at.near & bit != 0 {
                WAKE_RELEASE
            } else {
                WAKE_REACH
            };
            (d.is_finite() && d <= limit).then_some((d, bit))
        })
        .collect();
    near.sort_by(|a, b| a.0.total_cmp(&b.0));
    Reach {
        near: near
            .iter()
            .take(WAKE_MAX)
            .fold(0, |bits, &(_, bit)| bits | bit),
    }
}

/// The rooms of [`Reach::near`] bits around `room`.
pub fn reach_rooms(room: (i32, i32), near: u8) -> Vec<(i32, i32)> {
    DOORWAYS
        .iter()
        .filter(|(bit, _)| near & bit != 0)
        .map(|&(_, (dx, dy))| (room.0 + dx, room.1 + dy))
        .collect()
}

/// May a principal last seen in room `from` be granted `wake` for room
/// `to` (an observer ticket)? Only other rooms within [`WAKE_DOORS`]
/// doorways.
pub fn wake_allowed(from: (i32, i32), to: (i32, i32)) -> bool {
    let doors = from.0.abs_diff(to.0).saturating_add(from.1.abs_diff(to.1));
    (1..=WAKE_DOORS).contains(&doors)
}

#[cfg(test)]
mod tests {
    #[test]
    fn ring_dirs_go_evenly_round_the_circle() {
        for n in [4, 8, 16, 32, 64] {
            let dirs = super::ring_dirs(n);
            assert_eq!(dirs.len(), n);
            for (k, &(x, y)) in dirs.iter().enumerate() {
                let a = std::f32::consts::TAU * k as f32 / n as f32;
                assert!(
                    (x - a.cos()).abs() < 1e-5 && (y - a.sin()).abs() < 1e-5,
                    "{n}/{k}: {x},{y}"
                );
            }
        }
        assert_eq!(
            super::ring_dirs(20).len(),
            32,
            "rounded up to a power of two"
        );
    }

    use super::*;

    #[test]
    fn proximity_wake_reaches_doorways_only_two_at_most_with_hysteresis() {
        let reach = |x: f32, y: f32, near: u8| {
            rooms_in_reach(&ReachContext {
                x,
                y,
                near,
                open: 0,
            })
            .near
        };
        // The middle of the room: nothing, and it releases everything.
        assert_eq!(reach(0.0, 0.0, 0), 0);
        assert_eq!(reach(0.0, 0.0, 15), 0);
        // Walking east toward the doorway: wakes within WAKE_REACH.
        assert_eq!(reach(20.0 - WAKE_REACH - 0.1, 0.0, 0), 0);
        assert_eq!(reach(20.0 - WAKE_REACH + 0.1, 0.0, 0), 1);
        assert_eq!(reach(19.0, 0.0, 0), 1);
        // In the doorway passage (past the wall line), still east only.
        assert_eq!(reach(21.5, 0.0, 0), 1);
        // Walking back: stays near until beyond WAKE_RELEASE.
        assert_eq!(reach(20.0 - WAKE_RELEASE + 0.1, 0.0, 1), 1);
        assert_eq!(reach(20.0 - WAKE_RELEASE - 0.1, 0.0, 1), 0);
        // A room corner: no doorway is in reach (diagonals never wake).
        assert_eq!(reach(18.5, 18.5, 0), 0);
        // Every spot of the room (and the hysteresis from any prior set):
        // at most two, never two opposite ones.
        let mut seen_two = false;
        for prior in 0..16u8 {
            for i in 0..=88 {
                for j in 0..=88 {
                    let (x, y) = (-22.0 + i as f32 * 0.5, -22.0 + j as f32 * 0.5);
                    let near = reach(x, y, prior);
                    assert!(
                        near.count_ones() as usize <= WAKE_MAX,
                        "({x}, {y}) {prior:#b}"
                    );
                    assert!(
                        near & 3 != 3 && near & 12 != 12,
                        "opposite doorways at ({x}, {y})"
                    );
                    seen_two |= near.count_ones() == 2;
                }
            }
        }
        assert!(seen_two, "two doorways of a corner can both be near");
        // Walking a corner path east then north holds both briefly.
        let mut near = 0;
        for (x, y) in [(15.0, 0.0), (12.0, 6.0), (6.0, 11.0)] {
            near = reach(x, y, near);
        }
        assert_eq!(near, 1 | 4);
        assert_eq!(reach_rooms((3, -2), near), vec![(4, -2), (3, -1)]);
    }

    #[test]
    fn wake_is_granted_only_within_two_doorways() {
        assert!(wake_allowed((0, 0), (1, 0)));
        assert!(wake_allowed((0, 0), (2, 0)), "the next room's neighbour");
        assert!(wake_allowed((0, 0), (1, 1)));
        assert!(
            !wake_allowed((0, 0), (0, 0)),
            "the own room needs no watcher"
        );
        assert!(!wake_allowed((0, 0), (3, 0)));
        assert!(!wake_allowed((0, 0), (2, 1)));
        assert!(!wake_allowed((MAX_ROOM, 0), (-MAX_ROOM, 0)));
    }

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
    /// Floor cells reachable from a spawn point, every ramp counted as a
    /// wall (the strictest case: a steep one can't be climbed).
    fn reachable(obstacles: &[Obstacle], ramps: &[Ramp]) -> Vec<Vec<bool>> {
        let n = 79usize; // -19.5 ..= 19.5
        let at = |i: usize| -19.5 + i as f32 * 0.5;
        let r = arena::PLAYER_RADIUS;
        let free = |x: f32, y: f32| {
            let lim = arena::HALF - r;
            x.abs() <= lim
                && y.abs() <= lim
                && obstacles.iter().all(|o| {
                    let (dx, dy) = (x - o.x, y - o.y);
                    dx * dx + dy * dy >= (o.r + r).powi(2)
                })
                && ramps
                    .iter()
                    .all(|q| x < q.x0 - r || x > q.x1 + r || y < q.y0 - r || y > q.y1 + r)
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
                assert!(
                    (1..=MAX_RAMPS).contains(&layout.ramps.len()),
                    "pit.{x}.{y}: {} ramps",
                    layout.ramps.len()
                );
                let seen = reachable(&layout.obstacles, &layout.ramps);
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
                &Colliders::new(),
                0,
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
    fn ramps_are_seeded_slopes_of_random_whole_degrees() {
        let mut degrees = std::collections::BTreeSet::new();
        let (mut walkable, mut steep) = (0, 0);
        for y in -8..=8 {
            for x in -8..=8 {
                let ramps = room_ramps(DEFAULT_SEED, (x, y));
                assert_eq!(ramps, room_ramps(DEFAULT_SEED, (x, y)));
                for r in &ramps {
                    assert!(
                        (RAMP_DEGREES.0..=RAMP_DEGREES.1).contains(&r.degrees),
                        "{r:?}"
                    );
                    // Its height over its run is the slope it says.
                    let run = if r.uphill < 2 {
                        r.x1 - r.x0
                    } else {
                        r.y1 - r.y0
                    };
                    let tan = TAN[usize::from(r.degrees - RAMP_DEGREES.0)];
                    assert!((r.height / run - tan).abs() < 1e-3, "{r:?}");
                    assert!(r.height > 0.0 && r.height <= 2.2 + 1e-3, "{r:?}");
                    // The collider's normal agrees with walkable().
                    let (_, n) = r
                        .collider()
                        .top_at([(r.x0 + r.x1) / 2.0, (r.y0 + r.y1) / 2.0])
                        .unwrap();
                    assert_eq!(collide::walkable(n), r.walkable(), "{r:?} {n:?}");
                    if r.walkable() {
                        walkable += 1;
                    } else {
                        steep += 1;
                    }
                    degrees.insert(r.degrees);
                }
            }
        }
        assert!(degrees.len() > 30, "{degrees:?}");
        assert!(
            walkable > 50 && steep > 20,
            "{walkable} walkable, {steep} steep"
        );
        assert_ne!(
            room_ramps(DEFAULT_SEED, (0, 0)),
            room_ramps(DEFAULT_SEED, (1, 0))
        );
    }

    /// A room with one ramp climbing +x from x = 0, `degrees` steep, 2 m
    /// high, 4 m wide.
    fn one_ramp(degrees: u8) -> (Ramp, Colliders) {
        let run = 2.0 / TAN[usize::from(degrees - RAMP_DEGREES.0)];
        let ramp = Ramp {
            x0: 0.0,
            y0: -2.0,
            x1: run,
            y1: 2.0,
            height: 2.0,
            uphill: 0,
            degrees,
        };
        (ramp, ramp_colliders(&[ramp]))
    }

    fn on_ramps(s: PitState, mx: i8, my: i8, buttons: u8, ramps: &Colliders) -> PitState {
        step_among(
            s,
            PitInput {
                mx,
                my,
                buttons,
                ..PitInput::default()
            },
            &[],
            ramps,
            0,
        )
    }

    /// A 1 m platform over `x0..x1` (y −3..3).
    fn ledge(x0: f32, x1: f32) -> Colliders {
        Colliders::new().with(
            Platform {
                x0,
                y0: -3.0,
                x1,
                y1: 3.0,
                height: 1.0,
            }
            .collider(),
        )
    }

    #[test]
    fn coyote_time_allows_a_jump_three_ticks_off_a_ledge_once() {
        let ground = ledge(-6.0, 0.0);
        // Walk east off the platform; stop at `off` ticks in the air.
        let walk_off = |off: u8| {
            let mut s = canonicalize(PitState {
                x: -1.0,
                z: 1.0,
                vx: arena::MAX_SPEED,
                ..PitState::default()
            });
            for _ in 0..60 {
                if s.air == off && s.x > 0.0 {
                    return s;
                }
                s = on_ramps(s, 127, 0, 0, &ground);
            }
            panic!("never {off} ticks off: {s:?}")
        };
        // Its 3rd tick off the ledge still jumps (air counts this tick).
        let late = on_ramps(walk_off(2), 127, 0, PitInput::JUMP, &ground);
        assert!(late.vz > 0.0 && late.air == JUMPED, "coyote jump: {late:?}");
        // The window was spent: a second press in the air does nothing.
        let released = on_ramps(late, 127, 0, 0, &ground);
        let again = on_ramps(released, 127, 0, PitInput::JUMP, &ground);
        assert!(again.vz < released.vz, "no second jump: {again:?}");
        // Its 4th tick off is too late.
        let too_late = on_ramps(walk_off(3), 127, 0, PitInput::JUMP, &ground);
        assert!(too_late.vz <= 0.0, "past coyote time: {too_late:?}");
    }

    #[test]
    fn a_jump_press_is_buffered_and_one_press_gives_one_jump() {
        let flat = Colliders::new();
        // Falling: press, release, and land within the buffer.
        let fall = |ticks_before: u32| {
            let mut s = canonicalize(PitState {
                z: 0.5,
                ..PitState::default()
            });
            // Ticks until landing from here (no press).
            let mut probe = s;
            let mut land = 0;
            while probe.z > 0.0 {
                probe = on_ramps(probe, 0, 0, 0, &flat);
                land += 1;
            }
            let press_at = land - ticks_before;
            let mut jumped = false;
            for t in 0..land + 6 {
                let b = if t == press_at { PitInput::JUMP } else { 0 };
                s = on_ramps(s, 0, 0, b, &flat);
                jumped |= s.vz > 0.0;
            }
            jumped
        };
        assert!(fall(1), "pressed a tick before landing");
        assert!(fall(2), "pressed two ticks before landing");
        assert!(!fall(4), "pressed too early");
        // Holding jump from standing: one jump, not one per landing.
        let mut s = PitState::default();
        let mut takeoffs = 0;
        for _ in 0..90 {
            let before = s.vz;
            s = on_ramps(s, 0, 0, PitInput::JUMP, &flat);
            takeoffs += u32::from(s.vz > 0.0 && before <= 0.0 && s.z <= 0.5);
        }
        assert_eq!(takeoffs, 1, "held: one jump");
    }

    #[test]
    fn ground_speed_accelerates_stops_and_turns_with_less_grip_in_the_air() {
        let flat = Colliders::new();
        let ticks = |mut s: PitState, mx: i8, until: &dyn Fn(&PitState) -> bool| {
            for t in 1..=30 {
                s = on_ramps(s, mx, 0, 0, &flat);
                if until(&s) {
                    return t;
                }
            }
            99
        };
        let full = arena::MAX_SPEED - 0.01;
        // To full speed in 0.15 s (4.5 ticks).
        assert_eq!(ticks(PitState::default(), 127, &|s| s.vx >= full), 5);
        let running = canonicalize(PitState {
            vx: arena::MAX_SPEED,
            ..PitState::default()
        });
        // To a stop in 0.1 s with the stick let go.
        assert_eq!(ticks(running, 0, &|s| s.vx.abs() < 0.05), 3);
        // Twice as fast turning against it.
        assert_eq!(ticks(running, -127, &|s| s.vx < 0.05), 2);
        // Half the grip in the air.
        let one = on_ramps(PitState::default(), 127, 0, 0, &flat).vx;
        let airborne = on_ramps(
            canonicalize(PitState {
                z: 1.0,
                ..PitState::default()
            }),
            127,
            0,
            0,
            &flat,
        )
        .vx;
        assert!((airborne - one / 2.0).abs() < 0.01, "{airborne} vs {one}");
    }

    #[test]
    fn a_jump_just_short_of_a_ledge_is_lifted_onto_it() {
        let ground = ledge(0.0, 4.0);
        // Feet `under` below the 1 m top, rising a little, running at it.
        let run = |under: f32| {
            let mut s = canonicalize(PitState {
                x: -0.7,
                z: 1.0 - under,
                vz: 1.0,
                vx: arena::MAX_SPEED,
                air: JUMPED,
                ..PitState::default()
            });
            for _ in 0..12 {
                s = on_ramps(s, 127, 0, 0, &ground);
            }
            s
        };
        let close = run(0.25);
        assert!(
            close.x > 0.5 && (close.z - 1.0).abs() < 0.01,
            "lifted on: {close:?}"
        );
        let short = run(0.5);
        assert!(short.x <= 0.0 && short.z < 1.0, "too low: {short:?}");
    }

    #[test]
    fn a_walkable_ramp_is_climbed_and_walked_down() {
        let (ramp, ramps) = one_ramp(30);
        let mut s = canonicalize(PitState {
            x: -2.0,
            ..PitState::default()
        });
        let mut top = 0.0f32;
        for _ in 0..90 {
            s = on_ramps(s, 127, 0, 0, &ramps);
            if s.x < ramp.x1 - 0.1 {
                // On the slope the feet are on its surface.
                let (g, _) = ground(&ramps, s.x, s.y);
                assert!((s.z - g).abs() < 0.01, "{s:?} ground {g}");
            }
            top = top.max(s.z);
        }
        assert!(top > 1.9, "climbed to {top}");
        // Off the high end: a fall to the floor.
        assert!(s.x > ramp.x1 && s.z == 0.0, "{s:?}");
        // And back: the high end is a wall from behind.
        for _ in 0..60 {
            s = on_ramps(s, -127, 0, 0, &ramps);
        }
        assert!(
            s.x >= ramp.x1 + arena::PLAYER_RADIUS - 0.01 && s.z == 0.0,
            "{s:?}"
        );
        // A jump from the slope.
        let mut on = canonicalize(PitState {
            x: ramp.x1 / 2.0,
            z: 1.0,
            ..PitState::default()
        });
        on = on_ramps(on, 0, 0, 0, &ramps);
        let rest = on.z;
        on = on_ramps(on, 0, 0, PitInput::JUMP, &ramps);
        assert!(on.z > rest && on.vz > 0.0, "{on:?}");
    }

    #[test]
    fn a_steep_ramp_is_a_wall_you_slide_off_and_bounce_off() {
        let (ramp, ramps) = one_ramp(55);
        let mut s = canonicalize(PitState {
            x: -2.0,
            ..PitState::default()
        });
        for _ in 0..90 {
            s = on_ramps(s, 127, 0, 0, &ramps);
        }
        // Stopped at its low edge, still on the floor.
        assert!(s.x < ramp.x0 + 0.05 && s.z < 0.2, "{s:?}");
        // Dropped onto its middle, slowly: slides down and off, no jump
        // (long off any floor: no coyote time either).
        let mut on = canonicalize(PitState {
            x: (ramp.x0 + ramp.x1) / 2.0,
            z: 1.05,
            air: JUMPED,
            ..PitState::default()
        });
        let mut frames = 0;
        while on.x >= ramp.x0 {
            let jumped = on_ramps(on, 0, 0, PitInput::JUMP, &ramps);
            assert!(jumped.vz <= 0.0, "no jump off a steep slope: {jumped:?}");
            on = on_ramps(on, 0, 0, 0, &ramps);
            frames += 1;
            assert!(frames < 30, "slides off: {on:?}");
        }
        assert!(on.z < 0.3, "{on:?}");
        // Falling onto it fast: the velocity is reflected about its normal
        // (half kept along it), so it is thrown off downhill, away from it.
        let falling = canonicalize(PitState {
            x: (ramp.x0 + ramp.x1) / 2.0,
            z: 1.1,
            vz: -9.0,
            ..PitState::default()
        });
        let bounced = on_ramps(falling, 0, 0, 0, &ramps);
        let (_, n) = ground(&ramps, falling.x, falling.y);
        let away = bounced.vx * n[0] + bounced.vy * n[1] + bounced.vz * n[2];
        // Gravity acts for the frame before it lands.
        let into = -(9.0 + GRAVITY * arena::DT) * n[2];
        assert!(bounced.vx < -3.0, "{bounced:?}");
        assert!(
            (away + BOUNCE_RESTITUTION * into).abs() < 0.05,
            "{away} vs {into}: {bounced:?}"
        );
    }

    #[test]
    fn steps_are_the_same_with_and_without_the_ramp_far_away() {
        // Flat floor far from any ramp: ramps change nothing.
        let (_, ramps) = one_ramp(20);
        let mut a = canonicalize(PitState {
            x: -10.0,
            y: 10.0,
            ..PitState::default()
        });
        let mut b = a;
        for i in 0..120u32 {
            let input = PitInput {
                mx: if i % 40 < 20 { 90 } else { -90 },
                my: 30,
                buttons: if i % 33 == 0 { PitInput::JUMP } else { 0 },
                ..PitInput::default()
            };
            a = step_among(a, input, &[], &Colliders::new(), 0);
            b = step_among(b, input, &[], &ramps, 0);
            assert_eq!(a, b, "frame {i}");
        }
    }

    #[test]
    fn a_laser_path_bounces_off_the_walls() {
        // From the entrance's centre east, a little down: off the floor,
        // then on until the walls (or the doorway) end it.
        let path = laser_path(&LaserShot {
            seed: DEFAULT_SEED,
            room_x: 0,
            room_y: 0,
            x: 0.0,
            y: 3.0,
            z: 0.6,
            dx: 1.0,
            dy: 0.3,
            dz: -0.05,
        });
        assert!(
            !path.is_empty() && path.len() <= LASER_BOUNCES + 1,
            "{path:?}"
        );
        let first = path[0];
        assert!(
            first.nz > 0.99 || first.nx.abs() > 0.0 || first.ny.abs() > 0.0,
            "{path:?}"
        );
        // Normals of the bounces are unit; the stop has none.
        for h in &path {
            let n = (h.nx * h.nx + h.ny * h.ny + h.nz * h.nz).sqrt();
            assert!((n - 1.0).abs() < 1e-3 || n == 0.0, "{h:?}");
        }
        // A shot straight at the floor comes straight back up.
        let down = laser_path(&LaserShot {
            seed: DEFAULT_SEED,
            room_x: 0,
            room_y: 0,
            x: 4.5,
            y: 4.5,
            z: 1.5,
            dz: -1.0,
            ..LaserShot::default()
        });
        assert!((down[0].z).abs() < 1e-4 && down[0].nz > 0.99, "{down:?}");
    }

    #[test]
    fn terrain_rolls_meets_level_at_the_walls_and_has_holes_in_some_rooms() {
        let (mut hilly, mut holed, mut platforms) = (0, 0, 0);
        for y in -6..=6 {
            for x in -6..=6 {
                let at = (x, y);
                let field = room_terrain(DEFAULT_SEED, at);
                assert_eq!(field, room_terrain(DEFAULT_SEED, at));
                let top = field
                    .samples()
                    .iter()
                    .copied()
                    .filter(|&h| h != HOLE)
                    .fold(0.0f32, f32::max);
                if top > 0.3 {
                    hilly += 1;
                }
                assert!(top < 3.0, "pit.{x}.{y}: {top}");
                // Level along every wall and in the passages.
                for t in [-19.5f32, -10.0, 0.0, 7.5, 19.5] {
                    for (px, py) in [(t, 19.9), (t, -19.9), (19.9, t), (-19.9, t), (21.0, 0.0)] {
                        if let Some((h, _)) = field.height_at([px, py]) {
                            assert!(h.abs() < 1e-3, "pit.{x}.{y} ({px}, {py}): {h}");
                        }
                    }
                }
                let holes = room_holes(DEFAULT_SEED, at);
                if !holes.is_empty() {
                    holed += 1;
                }
                // Spawn points and the boss's home stand on ground.
                for k in KEEP {
                    assert!(
                        field.height_at([k.0, k.1]).is_some(),
                        "pit.{x}.{y}: {k:?} over a hole"
                    );
                }
                let layout = pit_room(&PitRoomContext {
                    seed: DEFAULT_SEED,
                    x,
                    y,
                });
                for m in &layout.mobs {
                    assert!(
                        holes.iter().all(|h| !h.contains(m.x, m.y)),
                        "pit.{x}.{y}: mob home {m:?} in a hole"
                    );
                }
                for h in &holes {
                    let w = (h.x1 - h.x0).min(h.y1 - h.y0);
                    assert!((HOLE_WIDTH.0..=HOLE_WIDTH.1 + 1e-3).contains(&w), "{h:?}");
                }
                platforms += layout.platforms.iter().filter(|p| p.height > 0.0).count();
            }
        }
        assert!(hilly > 60 && hilly < 160, "{hilly} hilly rooms");
        assert!(holed > 25 && holed < 90, "{holed} rooms with holes");
        assert!(platforms > 169, "{platforms} platforms");
        assert!(room_holes(DEFAULT_SEED, (0, 0)).is_empty());
    }

    /// A flat test room with a 3 m trench across y 6..9, a 1 m platform at
    /// x 6..8 and a 1 m hill at (-8, -8).
    fn test_ground() -> Colliders {
        Colliders::new()
            .with(Collider::aabb([6.0, -2.0], [8.0, 0.0], 1.0))
            .with_terrain(
                Heightfield::new([-22.0, -22.0], TERRAIN_CELL, 89, 89, |x, y| {
                    if y > 6.0 && y < 9.0 {
                        return HOLE;
                    }
                    let (dx, dy) = (x + 8.0, y + 8.0);
                    let u = (dx * dx + dy * dy) / 25.0;
                    if u < 1.0 {
                        (1.0 - u) * (1.0 - u)
                    } else {
                        0.0
                    }
                }),
                LAYER_FLOOR,
            )
    }

    #[test]
    fn obstacles_stand_on_level_ground_at_full_height() {
        for x in -6..6 {
            for y in -6..6 {
                let terrain = room_terrain(DEFAULT_SEED, (x, y));
                let levels = obstacle_levels(DEFAULT_SEED, (x, y));
                let colliders = room_colliders(DEFAULT_SEED, (x, y));
                for (o, level) in room_obstacles(DEFAULT_SEED, (x, y)).1.iter().zip(levels) {
                    for (dx, dy) in RING8 {
                        let (px, py) = (o.x + dx * (o.r + 0.3), o.y + dy * (o.r + 0.3));
                        let h = terrain.height_at([px, py]).map_or(level, |(h, _)| h);
                        // Level (where two patches meet, a few cm between them;
                        // near a wall it slopes with the walls' fade).
                        let near_wall = arena::HALF - o.x.abs().max(o.y.abs()) < o.r + 5.0;
                        let tolerance = if near_wall { 0.25 } else { 0.1 };
                        assert!(
                            (h - level).abs() < tolerance,
                            "pit.{x}.{y}: {h} beside {o:?} at {level}"
                        );
                    }
                    // On a hill, not sunk in one: on a slope the ground
                    // rises one side and falls the other, but a few metres
                    // round it is not higher all round.
                    let ring: Vec<f32> = RING8
                        .iter()
                        .filter_map(|(dx, dy)| {
                            let (px, py) = (o.x + dx * (o.r + 3.0), o.y + dy * (o.r + 3.0));
                            terrain.height_at([px, py]).map(|(h, _)| h)
                        })
                        .collect();
                    let around = ring.iter().sum::<f32>() / ring.len().max(1) as f32;
                    assert!(
                        around - level < 0.5,
                        "pit.{x}.{y}: {o:?} at {level} sunk in a {around} m hill"
                    );
                    // Its top stands its full height over its ground.
                    let top = colliders
                        .ground_at([o.x, o.y], LAYER_SOLID)
                        .map_or(0.0, |g| g.height);
                    assert!(
                        (top - level - OBSTACLE_HEIGHT).abs() < 1e-3,
                        "pit.{x}.{y}: top {top}"
                    );
                }
            }
        }
    }

    #[test]
    fn the_step_walks_over_a_hill_smoothly() {
        let ground = test_ground();
        let mut s = canonicalize(PitState {
            x: -14.0,
            y: -8.0,
            ..PitState::default()
        });
        let (mut top, mut last) = (0.0f32, 0.0f32);
        for _ in 0..80 {
            s = on_ramps(s, 127, 0, 0, &ground);
            let (g, _) = ground_of(&ground, s.x, s.y);
            assert!((s.z - g).abs() < 0.01, "on the surface: {s:?} vs {g}");
            assert!((s.z - last).abs() < 0.12, "no jolt: {last} → {}", s.z);
            last = s.z;
            top = top.max(s.z);
        }
        assert!(top > 0.95, "over the top: {top}");
        assert!(s.x > -3.0, "kept walking: {s:?}");
    }

    fn ground_of(g: &Colliders, x: f32, y: f32) -> (f32, [f32; 3]) {
        ground(g, x, y)
    }

    #[test]
    fn a_hole_swallows_a_walker_and_a_running_jump_clears_it() {
        let ground = test_ground();
        // Walking in: down to the bottom, and no climbing out.
        let mut s = canonicalize(PitState {
            x: 0.0,
            y: 3.0,
            ..PitState::default()
        });
        for _ in 0..60 {
            s = on_ramps(s, 0, 127, 0, &ground);
        }
        assert_eq!(s.z, arena::snap(HEIGHT, FALL_FLOOR), "{s:?}");
        let stuck = s;
        for _ in 0..30 {
            s = on_ramps(s, 0, -127, 0, &ground);
        }
        // Along its floor, but never up its rim.
        assert!(
            s.y > 5.5 && s.z < -3.0,
            "trapped: {s:?} (fell at {stuck:?})"
        );
        // A running jump from the edge clears 3 m.
        let mut s = canonicalize(PitState {
            x: 0.0,
            y: 0.0,
            ..PitState::default()
        });
        for _ in 0..60 {
            let near_edge = s.y > 5.6;
            s = on_ramps(
                s,
                0,
                127,
                if near_edge { PitInput::JUMP } else { 0 },
                &ground,
            );
            if s.z > 0.0 {
                break;
            }
        }
        for _ in 0..40 {
            s = on_ramps(s, 0, 127, 0, &ground);
        }
        assert!(s.y > 9.0 && s.z == 0.0, "made it across: {s:?}");
        // From further back (about 1.5 m short), only a jump-dash makes it.
        let run_up = |dash: bool| {
            let mut s = canonicalize(PitState {
                x: 0.0,
                y: 0.0,
                ..PitState::default()
            });
            let mut jumped = false;
            for _ in 0..80 {
                let jump = !jumped && s.y > 4.5;
                jumped |= jump;
                let buttons = if jump {
                    PitInput::JUMP
                } else if dash && jumped && s.z > 0.5 {
                    arena::BUTTON_DASH
                } else {
                    0
                };
                s = on_ramps(s, 0, 127, buttons, &ground);
            }
            s
        };
        assert!(run_up(false).z < -3.0, "a plain jump falls short");
        let dashed = run_up(true);
        assert!(
            dashed.z >= 0.0 && dashed.y > 9.0,
            "a jump-dash clears it: {dashed:?}"
        );
    }

    #[test]
    fn open_sides_are_shared_by_both_rooms_and_some_rooms_have_them() {
        let mut rooms_with_open = 0;
        for x in -8..8 {
            for y in -8..8 {
                let open = open_sides(DEFAULT_SEED, (x, y));
                // East of (x, y) is west of (x + 1, y); north is south of (x, y + 1).
                assert_eq!(open & 1 != 0, open_sides(DEFAULT_SEED, (x + 1, y)) & 2 != 0);
                assert_eq!(open & 4 != 0, open_sides(DEFAULT_SEED, (x, y + 1)) & 8 != 0);
                // The entrance and its neighbours keep their walls.
                if x.abs() + y.abs() < 2 {
                    assert_eq!(open, 0, "pit.{x}.{y}");
                }
                rooms_with_open += usize::from(open != 0);
                assert_eq!(
                    open,
                    pit_room(&PitRoomContext {
                        seed: DEFAULT_SEED,
                        x,
                        y
                    })
                    .open
                );
            }
        }
        assert!((60..200).contains(&rooms_with_open), "{rooms_with_open}");
    }

    #[test]
    fn an_open_side_is_crossed_anywhere_but_a_closed_one_only_at_its_door() {
        let walk = |open: u8, y: f32| {
            let mut s = canonicalize(PitState {
                x: 15.0,
                y,
                ..PitState::default()
            });
            for _ in 0..60 {
                s = step_among(
                    s,
                    PitInput {
                        mx: 127,
                        ..PitInput::default()
                    },
                    &[],
                    &Colliders::new(),
                    open,
                );
            }
            s
        };
        // Closed east side, 12 m off the doorway: held at the wall.
        let held = walk(0, 12.0);
        assert!(
            held.x <= arena::HALF - arena::PLAYER_RADIUS + SLACK,
            "{held:?}"
        );
        assert_eq!(crossing(held.x, held.y), None);
        // Open east side: through anywhere along it, into the neighbour.
        for y in [-19.0, -12.0, 0.0, 12.0, 19.0] {
            let through = walk(1, y);
            assert_eq!(
                crossing(through.x, through.y),
                Some((1, 0)),
                "y {y}: {through:?}"
            );
            assert!((through.y - y).abs() < 0.6, "{through:?}");
        }
        // A corner stays walled even with both sides open.
        let mut s = canonicalize(PitState {
            x: 15.0,
            y: 15.0,
            ..PitState::default()
        });
        for _ in 0..60 {
            s = step_among(
                s,
                PitInput {
                    mx: 127,
                    my: 127,
                    ..PitInput::default()
                },
                &[],
                &Colliders::new(),
                1 | 4,
            );
            let lim = arena::HALF - arena::PLAYER_RADIUS + SLACK;
            assert!(s.x.abs() <= lim || s.y.abs() <= lim, "{s:?}");
        }
    }

    #[test]
    fn reach_wakes_a_neighbour_anywhere_along_an_open_side() {
        let reach = |x: f32, y: f32, open: u8| {
            rooms_in_reach(&ReachContext {
                x,
                y,
                near: 0,
                open,
            })
            .near
        };
        // 12 m along the east wall, 3 m from it: too far from the doorway,
        // in reach of the open side.
        assert_eq!(reach(17.0, 12.0, 0) & 1, 0);
        assert_eq!(reach(17.0, 12.0, 1) & 1, 1);
        assert_eq!(reach(0.0, 0.0, 15), 0);
    }

    #[test]
    fn lasers_leave_through_doorways_never_over_a_wall() {
        let room = (-8..8)
            .flat_map(|x| (-8..8).map(move |y| (x, y)))
            .find(|&r| open_sides(DEFAULT_SEED, r) & 1 == 0)
            .expect("a room walled to the east");
        let colliders = room_colliders(DEFAULT_SEED, room);
        let h = arena::HALF;
        // Well above the wall and every obstacle, toward the east wall.
        let shot = |y: f32, z: f32| {
            colliders
                .raycast(&Ray::new([h - 1.0, y, z], [1.0, 0.0, 0.05], 10.0).mask(LASER_MASK))
                .map(|hit| hit.point[0])
        };
        for z in [WALL_HEIGHT + 0.1, 5.0, 40.0] {
            let at = shot(DOOR_HALF + 2.0, z).expect("the barrier stops it");
            assert!((at - h).abs() < 1e-3, "stopped at the wall line, not {at}");
            assert_eq!(shot(0.0, z), None, "the doorway lets it through");
        }
        // A shot that meets the barrier ends there: no bounce off thin air.
        let path = laser_path(&LaserShot {
            seed: DEFAULT_SEED,
            room_x: room.0,
            room_y: room.1,
            x: h - 1.0,
            y: DOOR_HALF + 2.0,
            z: 8.0,
            dx: 1.0,
            dy: 0.0,
            dz: 0.05,
        });
        assert_eq!(path.len(), 1, "{path:?}");
        assert!(
            (path[0].x - h).abs() < 1e-3 && path[0].nx < -0.9,
            "{path:?}"
        );
        // The barrier is never ground: the wall top stands where it was.
        let top = colliders.ground_at([h + 0.2, DOOR_HALF + 2.0], GROUND_MASK);
        assert_eq!(top.map(|g| g.height), Some(WALL_HEIGHT));
    }

    #[test]
    fn room_colliders_leave_out_the_walls_of_open_sides() {
        let room = (-8..8)
            .flat_map(|x| (-8..8).map(move |y| (x, y)))
            .find(|&r| open_sides(DEFAULT_SEED, r) & 1 != 0)
            .expect("a room open to the east");
        let colliders = room_colliders(DEFAULT_SEED, room);
        let ray = |y: f32| {
            colliders
                .raycast(&Ray::new([15.0, y, 1.0], [1.0, 0.0, 0.0], 10.0).mask(LAYER_SOLID))
                .map(|hit| hit.point[0])
        };
        // Through the open side at any height of the wall line; the corner
        // posts stand.
        assert_eq!(ray(12.0), None);
        assert_eq!(ray(-12.0), None);
        assert!(ray(arena::HALF + 0.2).is_some());
    }

    #[test]
    fn a_fast_move_cannot_tunnel_through_a_thin_wall() {
        // A 0.1 m slab 2 m tall between two points 1.2 m apart: the
        // end-point test alone misses it; the swept test sees it.
        let wall = Colliders::new()
            .with(Collider::aabb([-0.05, -3.0], [0.05, 3.0], 2.0).on_layers(LAYER_SOLID));
        let (from, to) = (
            (-0.6 - arena::PLAYER_RADIUS, 0.0),
            (0.6 + arena::PLAYER_RADIUS, 0.0),
        );
        assert!(!blocked(&wall, to.0, to.1, 0.0));
        assert!(swept_blocked(&wall, from, to, 0.0));
        assert!(!swept_blocked(
            &wall,
            from,
            (-0.1 - arena::PLAYER_RADIUS, 0.0),
            0.0
        ));
    }

    #[test]
    fn a_jump_lands_on_a_platform_and_its_side_blocks_a_walker() {
        let ground = test_ground();
        // Walking into it: stopped at its side.
        let mut s = canonicalize(PitState {
            x: 3.0,
            y: -1.0,
            ..PitState::default()
        });
        for _ in 0..40 {
            s = on_ramps(s, 127, 0, 0, &ground);
        }
        assert!(
            s.x <= 6.0 - arena::PLAYER_RADIUS + 0.01 && s.z == 0.0,
            "{s:?}"
        );
        // Jumping onto it.
        let mut frames = 0;
        s = on_ramps(s, 127, 0, PitInput::JUMP, &ground);
        while s.z > 1.0 + 1e-3 || s.vz != 0.0 {
            s = on_ramps(s, 64, 0, 0, &ground);
            frames += 1;
            assert!(frames < 60, "{s:?}");
        }
        assert!(
            (s.z - 1.0).abs() < 1e-3 && (6.0..=8.0).contains(&s.x),
            "on top: {s:?}"
        );
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
