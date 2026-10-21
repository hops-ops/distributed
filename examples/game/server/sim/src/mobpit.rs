//! Mob Pit's input frame, its client-predicted step (the Arena's
//! movement; combat is server-owned and never predicted) and its rooms.
//!
//! **Rooms.** Mob Pit is a near-infinite grid of rooms, room `(x, y)` being
//! zone `pit.<x>.<y>` (canonical integers, |x|, |y| ≤ [`MAX_ROOM`]). Every
//! room is the Arena floor (same pillars, so the predicted step is the
//! Arena's) with a doorway in the middle of each wall leading to the
//! neighbour. What lives in a room — its biome, danger level and mobs — is
//! a pure function of `(seed, x, y)` ([`pit_room`], `#[game_pure]`) built on
//! the Endless Grid's seeded hashing and biomes ([`crate::grid`]); the page
//! runs the same code compiled to WebAssembly (minimap colours, floor).

use distributed_game::{game_pure, predict, GameInput, GameRecord};

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
/// [`PitInput::movement`].
#[predict(PitStep, kind = "player")]
pub fn pit_step(state: &MoveState, input: &PitInput, _context: &(), _dt: f32) -> MoveState {
    arena::step(*state, input.movement())
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
    /// Doorway half width (m).
    pub door_half: f32,
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
        door_half: DOOR_HALF,
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

/// The doorway a player at `(x, y)` pushing with stick `(mx, my)` walks
/// through, as a room offset: against the wall, inside the doorway span,
/// pushing outward.
pub fn doorway(x: f32, y: f32, mx: i8, my: i8) -> Option<(i32, i32)> {
    let edge = arena::HALF - arena::PLAYER_RADIUS - 0.05;
    if y.abs() < DOOR_HALF {
        if x >= edge && mx > 30 {
            return Some((1, 0));
        }
        if x <= -edge && mx < -30 {
            return Some((-1, 0));
        }
    }
    if x.abs() < DOOR_HALF {
        if y >= edge && my > 30 {
            return Some((0, 1));
        }
        if y <= -edge && my < -30 {
            return Some((0, -1));
        }
    }
    None
}

/// Where a player arrives through the doorway it came in by (`from` is the
/// offset of the room it left, e.g. `(-1, 0)` = came from the west),
/// keeping its position along the wall.
pub fn arrival(from: (i32, i32), along: f32) -> (f32, f32) {
    let inside = arena::HALF - 2.0;
    let along = along.clamp(-(DOOR_HALF - 0.6), DOOR_HALF - 0.6);
    match from {
        (-1, 0) => (-inside, along),
        (1, 0) => (inside, along),
        (0, -1) => (along, -inside),
        _ => (along, inside),
    }
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
        let edge = arena::HALF - arena::PLAYER_RADIUS;
        assert_eq!(doorway(edge, 0.5, 127, 0), Some((1, 0)));
        assert_eq!(doorway(edge, 0.5, -127, 0), None, "pushing back in");
        assert_eq!(doorway(edge, 5.0, 127, 0), None, "wall, not doorway");
        assert_eq!(doorway(0.0, -edge, 0, -127), Some((0, -1)));
        assert_eq!(arrival((-1, 0), 9.0).0, -(arena::HALF - 2.0));
        assert!(arrival((-1, 0), 9.0).1 < DOOR_HALF);
    }
}
