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

use distributed_game::{game_pure, predict, FieldType, GameInput, GameRecord};

use crate::arena::{self, MoveInput, MoveState};
use crate::grid;

/// Input: the Arena's 3-byte frame (`[mx, my, buttons]`) with an attack
/// bit next to dash.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, GameInput)]
pub struct PitInput {
    /// Stick x.
    #[range(min = -127, max = 127)]
    pub mx: i8,
    /// Stick y.
    #[range(min = -127, max = 127)]
    pub my: i8,
    /// [`PitInput::DASH`] | [`PitInput::ATTACK`].
    #[buttons(dash, attack)]
    pub buttons: u8,
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
}

/// The own capsule's predicted step: the Arena's movement of
/// [`PitInput::movement`] inside walls with doorway passages.
#[predict(PitStep, kind = "player")]
pub fn pit_step(state: &MoveState, input: &PitInput, _context: &(), _dt: f32) -> MoveState {
    step(*state, *input)
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

/// One command frame: [`arena::advance`] (stick, dash, pillars), then the
/// walls with their doorway passages, snapped to the wire precision.
pub fn step(s: MoveState, input: PitInput) -> MoveState {
    let next = arena::advance(s, input.movement());
    canonicalize(walls(s, next))
}

/// Snap to the replicated precision of a Mob Pit player.
pub fn canonicalize(s: MoveState) -> MoveState {
    MoveState {
        x: arena::snap(POS, s.x),
        y: arena::snap(POS, s.y),
        vx: arena::snap(arena::VEL, s.vx),
        vy: arena::snap(arena::VEL, s.vy),
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
/// Mobs a room holds at most.
pub const MAX_MOBS: usize = 12;

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
    /// Mob slots (ids 1..).
    pub mobs: Vec<RoomMob>,
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
    let h = grid::mix(room.seed, SALT_ROOM, room.x, room.y);
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
    PitRoomLayout {
        biome,
        danger,
        mobs,
    }
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
pub fn into_neighbour(step: (i32, i32), s: MoveState) -> MoveState {
    canonicalize(MoveState {
        x: s.x - step.0 as f32 * ROOM_SIZE,
        y: s.y - step.1 as f32 * ROOM_SIZE,
        ..s
    })
}

/// Whether `s` is a position a player can be in (the room or a passage).
pub fn admissible(s: &MoveState) -> bool {
    let lim = arena::HALF - arena::PLAYER_RADIUS + SLACK;
    let gap = DOOR_HALF - arena::PLAYER_RADIUS + SLACK;
    let far = arena::HALF + PASSAGE + SLACK;
    let (ax, ay) = (s.x.abs(), s.y.abs());
    s.x.is_finite()
        && s.y.is_finite()
        && s.vx.is_finite()
        && s.vy.is_finite()
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
    fn room_ids_and_doorways() {
        assert_eq!(room_zone((1, -2)), "pit.1.-2");
        assert_eq!(parse_room("pit.1.-2"), Some((1, -2)));
        assert_eq!(parse_room("pit.01.2"), None);
        assert_eq!(parse_room("pit-1"), None);
        assert_eq!(parse_room("pit.2000000.0"), None);
    }

    fn walk(mut s: MoveState, mx: i8, my: i8, frames: usize) -> MoveState {
        for _ in 0..frames {
            s = step(s, PitInput { mx, my, buttons: 0 });
        }
        s
    }

    #[test]
    fn doorways_are_passages_and_walls_hold() {
        let lim = arena::HALF - arena::PLAYER_RADIUS;
        // Along the east door axis (y = 0.5): through the gap past the wall
        // line, up to the passage's end.
        let at = |x, y| MoveState {
            x,
            y,
            ..MoveState::default()
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
        let s = canonicalize(MoveState {
            x: arena::HALF + HYSTERESIS + 0.1,
            y: 1.25,
            vx: 5.0,
            ..MoveState::default()
        });
        let step = crossing(s.x, s.y).expect("past the line");
        let n = into_neighbour(step, s);
        // World x: room 0 + s.x == room 1 + n.x.
        assert!((n.x + ROOM_SIZE - s.x).abs() < 1e-3, "{n:?}");
        assert_eq!((n.y, n.vx), (s.y, s.vx));
        assert!(admissible(&n));
        assert_eq!(crossing(n.x, n.y), None, "hysteresis: no bounce back");
        // West / south mirror.
        let w = into_neighbour((-1, 0), MoveState { x: -21.0, ..s });
        assert!((w.x - 19.0).abs() < 1e-3, "{w:?}");
        let south = into_neighbour(
            (0, -1),
            MoveState {
                x: 0.0,
                y: -21.0,
                ..s
            },
        );
        assert!((south.y - 19.0).abs() < 1e-3, "{south:?}");
        assert!(!admissible(&MoveState {
            x: 21.0,
            y: 5.0,
            ..s
        }));
    }
}
