//! One Mob Pit room: authoritative melee combat at 30 Hz.
//!
//! Movement is the Arena's step through [`pit_step`], the declared predicted
//! step the client runs as WebAssembly; combat is server-only. Entities:
//!
//! | kind | ids | fields |
//! |---|---|---|
//! | 1 `player` | 1000 + player | x y vx vy dash cd (predicted) · name hp face swing level dead |
//! | 2 `mob`    | 1..=12 | x y face tier hp state tele |
//! | 3 `pillar` | 100 + i | x y r (global) |
//! | 4 `pit`    | 900 | pending committed backlog tickMs tickMaxMs kills (global) |
//!
//! The room's mobs come from its seeded layout ([`pit_room`]: tiers and
//! homes); the rest of a room is the Arena floor with a doorway in the
//! middle of each wall. The rooms tile one continuous world (room `(x, y)`
//! at `(x, y) × ROOM_SIZE`): a doorway is a passage the predicted step walks
//! through, and a player [`HYSTERESIS`](game_sim::mobpit::HYSTERESIS) past
//! the wall line is handed to the neighbouring room ([`Ctx::handoff`],
//! carrying its [`MoveState`] in the neighbour's frame: the same world
//! point and velocity); the client follows the Handoff to the next room's
//! zone, wherever it runs, and draws it where it always was.
//!
//! Mob state machine (deterministic, zone RNG only for wander targets):
//! `idle` (wander near home) → `chase` (living player within aggro radius or
//! hit by one) → `windup` (in reach: stand still, telegraph ring for 0.8 s) →
//! damage everyone inside the ring → `recover` (0.6 s) → `chase`; beyond the
//! leash or target dead → `return` home → `idle`. Dead mobs respawn after 4 s
//! with a new generation.
//!
//! A kill emits a typed fact **without awaiting storage**: the Crawler
//! cell command's own input, `RecordKillInput { mob, tier, name }`, keyed by
//! `m{mob}` and its generation and emitted for the killer (the host binds
//! the verified principal; the cell decides the XP). The settlement arrives
//! in [`Zone::on_fact_settled`] on a later tick and is pushed to the killer
//! as [`PitEvent::Saved`] + [`PitEvent::Profile`].
//!
//! **Hibernation.** [`Zone::hibernate`] keeps every mob (position, HP,
//! state, generation, respawn timer) and the room's kill count; a room that
//! wakes up restores them ([`Zone::on_restore`]) and tells joining players
//! ([`PitEvent::Hello::restored`]).
//!
//! **Coming back.** A reloaded page (a fresh ticket for a slot its
//! principal still holds) takes over the same body ([`Zone::on_rejoin`]
//! re-greets it). When a slot ends (closed page, grace expired, kicked) the
//! room parks the player's place and HP by verified principal; the
//! snapshot keeps the parked players and the connected ones, so a fresh
//! join after the room slept, or after celld restarted, resumes there
//! instead of at a spawn point. Nothing is written per tick.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Duration;

use distributed_game::rewind::{Historic, HitQuery, RewindFrame, ShotTiming};
use distributed_game::{
    schema, ConsumePolicy, Ctx, EmitError, Entities, Entity, EntityId, EntitySchema, Fact,
    FactOutcome, FactSettlement, GameEvent, GameInput, GameRecord, GameSurface, LeaveReason,
    ObserverId, PlayerId, Project, Replicated, View, Zone,
};
use game_sim::arena::{self as movement, PLAYER_RADIUS};
use game_sim::mobpit::PitState as MoveState;
use game_sim::mobpit::{
    self as rooms, admissible, borough_of, crossing, hood_of, into_neighbour, pair_shard,
    parse_room, pit_room, pit_world, room_zone, PitRoomContext, PitRoomFn, RoomsInReachFn,
    BOSS_TIER,
};
use mobpit_crawler::{level_floor, KillReceipt, RecordKillInput};
use mobpit_tiers::{BossDefeatedInput, WEATHER};
use serde::{Deserialize, Serialize};

pub use game_sim::mobpit::{pit_step, PitInput, PitStep};

/// Entity id of the stats entity (the zone's own HUD view).
pub const PIT_ENTITY: EntityId = EntityId(900);

/// A player: the predicted [`MoveState`] (the Arena capsule plus a jump's
/// height, `game_sim::mobpit::PitState`) plus server-owned combat
/// state.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "player", id = 1, priority = 2.0, of = Player)]
pub struct PlayerView {
    /// Position x (room-local; past ±HALF in a doorway passage). A
    /// misprediction eases out over 150 ms; a jump over 2 m (a respawn) is
    /// a cut, never a slide.
    #[field_type(rooms::POS)]
    #[correct(ease = 150, snap_above = 2.0)]
    #[from(state.x)]
    pub x: f32,
    /// Position y.
    #[field_type(rooms::POS)]
    #[correct(ease = 150, snap_above = 2.0)]
    #[from(state.y)]
    pub y: f32,
    /// Velocity x.
    #[field_type(movement::VEL)]
    #[correct(snap)]
    #[from(state.vx)]
    pub vx: f32,
    /// Velocity y.
    #[field_type(movement::VEL)]
    #[correct(snap)]
    #[from(state.vy)]
    pub vy: f32,
    /// Remaining dash frames.
    #[correct(snap)]
    #[from(state.dash)]
    pub dash: u8,
    /// Remaining dash cooldown frames.
    #[correct(snap)]
    #[from(state.cd)]
    pub cd: u8,
    /// Height above the floor (a jump).
    #[field_type(rooms::HEIGHT)]
    #[correct(ease = 100)]
    #[from(state.z)]
    pub z: f32,
    /// Vertical velocity.
    #[field_type(movement::VEL)]
    #[correct(snap)]
    #[from(state.vz)]
    pub vz: f32,
    /// Display name.
    #[priority(0.2)]
    #[correct(replace)]
    #[from]
    pub name: String,
    /// Hit points.
    #[correct(replace)]
    pub hp: u8,
    /// Facing (rad).
    #[angle(bits = 8)]
    #[correct(replace)]
    #[from]
    pub face: f32,
    /// Swing animation frames left.
    #[correct(replace)]
    pub swing: u8,
    /// Level (from the durable cell).
    #[correct(replace)]
    pub level: u16,
    /// Waiting to respawn.
    #[correct(replace)]
    pub dead: bool,
    /// Bow animation frames left (a ranged shot was loosed).
    #[correct(replace)]
    pub shot: u8,
}

/// A mob (dead mobs are not replicated).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "mob", id = 2, of = Mob)]
pub struct MobView {
    /// Position x.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    #[from]
    pub x: f32,
    /// Position y.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    #[from]
    pub y: f32,
    /// Facing (rad).
    #[angle(bits = 8)]
    #[from]
    pub face: f32,
    /// 1 grunt · 2 brute · 3 elite.
    #[from]
    pub tier: u8,
    /// HP percent.
    pub hp: u8,
    /// 0 idle · 1 chase · 2 windup · 3 recover · 4 return.
    pub state: u8,
    /// Telegraph progress 0..=255 during a windup.
    pub tele: u8,
}

/// A pillar of the floor (visible everywhere).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pillar {
    /// Entity id (100 + index).
    pub id: EntityId,
    /// Centre x.
    pub x: f32,
    /// Centre y.
    pub y: f32,
    /// Radius.
    pub r: f32,
}

impl Pillar {
    /// The obstacles of a room's seeded layout.
    pub fn of(obstacles: &[game_sim::mobpit::Obstacle]) -> Vec<Pillar> {
        obstacles
            .iter()
            .enumerate()
            .map(|(i, o)| Pillar {
                id: EntityId(100 + i as u32),
                x: o.x,
                y: o.y,
                r: o.r,
            })
            .collect()
    }
}

impl Entity for Pillar {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

/// A pillar, projected as is.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "pillar", id = 3, global, of = Pillar)]
pub struct PillarView {
    /// Centre x.
    #[from]
    pub x: f32,
    /// Centre y.
    #[from]
    pub y: f32,
    /// Radius.
    #[from]
    pub r: f32,
}

/// Room numbers for the HUD (global), projected from the zone itself.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "pit", id = 4, global, of = MobPit)]
pub struct PitView {
    /// Facts emitted and not settled.
    pub pending: u16,
    /// Settlements received.
    pub committed: u32,
    /// Facts waiting for backpressure to clear.
    pub backlog: u16,
    /// Tick interval EMA (ms).
    pub tick_ms: f32,
    /// Longest tick interval of the last 30 ticks (ms).
    pub tick_max_ms: f32,
    /// Mobs killed in this room (kept across hibernation).
    pub kills: u32,
}

/// A player's durable progress (from the cell on join, after each settlement).
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct Profile {
    /// Total XP.
    pub xp: u64,
    /// Level.
    pub level: u32,
    /// Kills.
    pub kills: u32,
    /// First XP of the level.
    pub level_xp: u64,
    /// First XP of the next level.
    pub next_level_xp: u64,
    /// `"cell"` (loaded on join) or `"settled"` (after a kill committed).
    pub source: String,
}

/// Mob Pit's events.
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum PitEvent {
    /// To a joining player.
    #[game_event(id = 1)]
    Hello {
        /// Own entity id.
        entity: u32,
        /// Floor half extent (m).
        half: f32,
        /// Melee reach (m).
        attack_range: f32,
        /// Cosine of the half cone angle.
        attack_arc_cos: f32,
        /// Room x.
        room_x: i32,
        /// Room y.
        room_y: i32,
        /// World seed of the rooms.
        seed: u32,
        /// This activation of the room restored a hibernated snapshot.
        restored: bool,
        /// Mobs killed in this room so far.
        room_kills: u32,
        /// Interest radius (m): what the room replicates around the player.
        view_radius: f32,
    },
    /// Durable progress.
    #[game_event(id = 2)]
    Profile(Profile),
    /// Broadcast the tick a mob dies (the fact is only queued).
    #[game_event(id = 3)]
    Kill {
        /// Fact key.
        key: String,
        /// Mob entity id.
        mob: u32,
        /// Mob tier.
        tier: u8,
        /// Where it died.
        x: f32,
        /// Where it died.
        y: f32,
        /// Killer entity id.
        killer: u32,
        /// Killer display name.
        killer_name: String,
    },
    /// To the killer when its fact settled.
    #[game_event(id = 4)]
    Saved {
        /// Fact key.
        key: String,
        /// Committed.
        ok: bool,
        /// The cell recognised the key (replayed receipt).
        replayed: bool,
        /// XP credited by this kill (decided by the cell).
        xp: u32,
        /// Total XP after it.
        total_xp: u64,
        /// Kills after it.
        kills: u32,
        /// Level after it.
        level: u32,
        /// Time from emit to settlement (ms).
        waited_ms: u32,
        /// Where the mob died.
        x: f32,
        /// Where the mob died.
        y: f32,
        /// Failure, if any.
        error: Option<String>,
        /// Commit attempts (failures).
        attempts: u32,
    },
    /// To a player that died.
    #[game_event(id = 6)]
    Died {
        /// Respawn delay (ms).
        respawn_ms: u32,
    },
    /// The world above the room (its hood, borough and city): on join and
    /// whenever it changes.
    #[game_event(id = 7)]
    World(WorldTiers),
    /// The verdict of one ranged shot (to the shooter).
    #[game_event(id = 9)]
    Shot {
        /// Client shot id.
        shot: u16,
        /// Mob hit.
        hit: Option<u32>,
        /// Lag compensation used.
        compensated: bool,
        /// Rewind applied (ms, 0 without compensation).
        age_ms: f32,
        /// Rewind the shooter asked for (ms).
        requested_age_ms: f32,
        /// Limited by the clamp or the stored history.
        clamped: bool,
        /// Where the arrow stopped (room-local).
        x: f32,
        /// Where the arrow stopped.
        y: f32,
        /// Where the arrow stopped (height).
        z: f32,
    },
    /// To a player whose client runs older content than the room (a live
    /// content update happened): only the entrance (a gate) lets it through,
    /// and reloads it there. Sent when it tries another doorway.
    #[game_event(id = 10)]
    Stale {
        /// The gate room (`pit.0.0`).
        gate: String,
        /// The world's content version.
        version: String,
    },
    /// The hood's boss fell here.
    #[game_event(id = 8)]
    BossDown {
        /// Display name of the slayer.
        by: String,
        /// The hood's tier now.
        hood_tier: u32,
    },
}

/// The tiers above a room, as its players see them.
#[derive(Debug, Clone, Default, PartialEq, GameRecord)]
pub struct WorldTiers {
    /// Hood x.
    pub hood_x: i32,
    /// Hood y.
    pub hood_y: i32,
    /// Times the hood's boss fell: how much tougher its rooms are.
    pub hood_tier: u32,
    /// The hood's boss is down.
    pub boss_down: bool,
    /// Who felled it last.
    pub last_by: String,
    /// Borough x.
    pub borough_x: i32,
    /// Borough y.
    pub borough_y: i32,
    /// Bosses down in the borough (of 9).
    pub borough_bosses_down: u32,
    /// The borough's weather (index into `WEATHER`).
    pub weather: u8,
    /// Hoods of the borough whose boss is down (`hx.hy`).
    pub hoods_down: Vec<String>,
    /// The borough's world event (`invasion`, `frenzy`, `blessing`; empty
    /// when none), scheduled by its cell.
    pub event: String,
    /// When the event ends (Unix ms, 0 when none).
    pub event_until_ms: u64,
    /// Bosses down across the city.
    pub city_bosses_down: u32,
    /// The city's banner.
    pub banner: String,
}

/// What services deliver into a room.
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum PitMessage {
    /// A player's durable progress, read from its Crawler cell after it
    /// joined (off the tick).
    #[game_event(id = 1)]
    Loaded {
        /// The player's verified principal.
        principal: String,
        /// Total XP.
        xp: u64,
        /// Level.
        level: u32,
        /// Kills.
        kills: u32,
    },
    /// The tiers above the room, read from their cells (off the tick).
    #[game_event(id = 2)]
    World(WorldTiers),
    /// The same without the city (not read this round: kept as it was).
    #[game_event(id = 3)]
    WorldKeepCity(WorldTiers),
    /// The room's hood, pushed by its cell (or re-read).
    #[game_event(id = 4)]
    Hood {
        /// Times its boss fell.
        tier: u32,
        /// Its boss is down.
        boss_down: bool,
        /// Who felled it last.
        last_by: String,
    },
    /// The room's borough, pushed by its cell (or re-read).
    #[game_event(id = 5)]
    Borough {
        /// Bosses down in the borough.
        bosses_down: u32,
        /// Weather (index into `WEATHER`).
        weather: u8,
        /// World event (empty when none).
        event: String,
        /// When it ends (Unix ms).
        event_until_ms: u64,
        /// Hoods whose boss is down (`hx.hy`).
        hoods_down: Vec<String>,
    },
    /// The city, pushed by its cell (or re-read).
    #[game_event(id = 6)]
    City {
        /// Bosses down across the city.
        bosses_down: u32,
        /// The city's banner.
        banner: String,
    },
}

/// Mob Pit's client surface (also used by the leaderboard page).
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<MobPit>("mobpit")
        .events::<PitEvent>()
        .predict::<PitStep>()
        .pure::<PitRoomFn>()
        .pure::<RoomsInReachFn>()
        .constant("pit_rules", &pit_rules())
        .constant("weather", &WEATHER.map(String::from).to_vec())
        .constant(
            "patterns",
            &game_sim::mobpit::PATTERNS.map(String::from).to_vec(),
        )
        .constant("pit_world", &pit_world())
        .constant("content_packs", &content_packs())
        .constant("gate_room", &GATE_ROOM.to_string())
}

/// Input bit: attack (bit 0 is the Arena's dash).
pub const BUTTON_ATTACK: u8 = PitInput::ATTACK;
/// Melee reach (m, from the player's centre to the mob's edge).
pub const ATTACK_RANGE: f32 = 2.2;
/// Cosine of the half cone angle (60°).
pub const ATTACK_ARC_COS: f32 = 0.5;
/// Damage per swing.
pub const ATTACK_DAMAGE: i32 = 10;
/// Swing cooldown (ticks).
pub const SWING_COOLDOWN: u8 = 13;
/// Swing animation length (ticks).
const SWING_FRAMES: u8 = 6;
/// Player max HP.
pub const PLAYER_HP: i32 = 100;
/// Inputs a player may bank (one per tick).
const MAX_CREDITS: u8 = 4;
/// Dropped ticks a player may bank: the room's input capacity, the most
/// that can be queued.
const MAX_OWED: u16 = 90;
/// Interest radius (m): mobs and players farther away are not replicated
/// (the page draws it as the interest ring). From any spawn point every mob
/// home is inside it; walk to a corner and the far side drops out.
pub const VIEW_RADIUS: f32 = 24.0;
/// What a watcher sees (from the room's centre: the whole room). On celld a
/// watcher is passive: it never keeps the room awake.
pub const WATCH_RADIUS: f32 = 30.0;
const AGGRO: f32 = 7.0;
const LEASH: f32 = 15.0;
const WINDUP_TICKS: u16 = 24;
const RECOVER_TICKS: u16 = 18;
const RESPAWN_TICKS: u64 = 120;
/// A player at least this high (m) jumps over a mob's slam.
pub const SLAM_CLEARANCE: f32 = 0.9;
/// Ranged reach (m).
pub const RANGE: f32 = 30.0;
/// Damage per arrow.
pub const RANGED_DAMAGE: i32 = 12;
/// Ticks between shots.
pub const FIRE_COOLDOWN: u8 = 12;
/// Bow animation frames.
const SHOT_FRAMES: u8 = 6;
/// Eye height above the feet (m): where first-person arrows leave from.
pub const EYE_HEIGHT: f32 = 1.5;
/// Where third-person (and top-down) arrows leave from, flying level (m).
pub const HIP_HEIGHT: f32 = 0.6;
/// Furthest a shot is rewound (ms): the anti-cheat clamp.
pub const MAX_REWIND_MS: u64 = 600;
/// Obstacles stop arrows below this height (m).
const OBSTACLE_HEIGHT: f32 = 2.4;
/// Mob body height by tier (m): their hit capsule.
fn mob_height(tier: u8) -> f32 {
    match tier {
        4 => 2.8,
        3 => 2.0,
        2 => 1.5,
        _ => 0.9,
    }
}
const PLAYER_RESPAWN_TICKS: u64 = 90;

/// One content pack: what a content version changes in the rooms (mob
/// pace and toughness) and how clients running it draw them (the tint).
/// The live version comes from the world's content rollout
/// (`distributed_game::world::content_rollout`); the generated
/// `CONTENT_PACKS` gives the pages the same table.
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct ContentPack {
    /// Content version (`""`: the built-in pack).
    pub version: String,
    /// Display name.
    pub name: String,
    /// Mob speed multiplier.
    pub mob_pace: f32,
    /// Mob hit points, percent of the base.
    pub mob_hp_pct: u16,
    /// Mob tint (0xRRGGBB, 0: none).
    pub tint: u32,
    /// Default change notes.
    pub notes: String,
}

/// Every content pack, the built-in one first.
pub fn content_packs() -> Vec<ContentPack> {
    let pack = |version: &str, name: &str, mob_pace, mob_hp_pct, tint, notes: &str| ContentPack {
        version: version.into(),
        name: name.into(),
        mob_pace,
        mob_hp_pct,
        tint,
        notes: notes.into(),
    };
    vec![
        pack("", "Classic", 1.0, 100, 0, ""),
        pack(
            "frostfall",
            "Frostfall",
            0.7,
            120,
            0x8fd3ff,
            "Frostfall: the mobs are frozen stiff (30 % slower) but tougher (+20 % HP).",
        ),
        pack(
            "emberfall",
            "Emberfall",
            1.25,
            85,
            0xff7a3a,
            "Emberfall: the mobs burn hot (25 % faster) and brittle (-15 % HP).",
        ),
    ]
}

/// The pack of `version` (unknown versions run the built-in pack).
pub fn content_pack(version: &str) -> ContentPack {
    let packs = content_packs();
    packs
        .iter()
        .find(|p| p.version == version)
        .cloned()
        .unwrap_or_else(|| packs[0].clone())
}

/// The gate room: stale clients reload when they walk into it.
pub const GATE_ROOM: &str = "pit.0.0";

/// Mob tier stats: (max hp, speed m/s, damage, radius, telegraph radius).
fn tier_stats(tier: u8) -> (i32, f32, i32, f32, f32) {
    match tier {
        4 => (400, 2.2, 22, 1.2, 3.2),
        3 => (100, 3.3, 24, 0.8, 2.6),
        2 => (60, 2.4, 16, 0.7, 2.2),
        _ => (30, 3.0, 9, 0.5, 1.8),
    }
}

/// Mob tiers (1..=3, and 4: a hood's boss).
pub const TIERS: [u8; 4] = [1, 2, 3, BOSS_TIER];
/// A fallen boss stays down at least this long here (its hood's schedule
/// brings it back; the hood's state is read every few seconds).
const BOSS_RESPAWN_TICKS: u64 = 30 * mobpit_tiers::BOSS_RESPAWN_MS / 1000;

/// Hit points of a mob of `tier` in a room of hood tier `difficulty`
/// (each boss kill in the hood makes its rooms 25 % tougher, up to
/// `mobpit_tiers::MAX_TIER`).
fn max_hp(tier: u8, difficulty: u32) -> i32 {
    let d = difficulty.min(mobpit_tiers::MAX_TIER) as i32;
    tier_stats(tier).0 * (4 + d) / 4
}

/// [`max_hp`] under a content pack.
fn pack_hp(tier: u8, difficulty: u32, pack: &ContentPack) -> i32 {
    (max_hp(tier, difficulty) * i32::from(pack.mob_hp_pct) / 100).max(1)
}

/// Damage of a mob of `tier` at hood tier `difficulty` (+20 % per tier).
fn damage(tier: u8, difficulty: u32) -> i32 {
    let d = difficulty.min(mobpit_tiers::MAX_TIER) as i32;
    tier_stats(tier).2 * (5 + d) / 5
}
/// Names of the `mob` kind's `state` values, by value.
pub const MOB_STATES: [&str; 6] = ["idle", "chase", "windup", "recover", "return", "dead"];

/// What the page draws of a mob tier (from [`tier_stats`]).
#[derive(Debug, Clone, Copy, PartialEq, GameRecord)]
pub struct MobTier {
    /// Tier (the `mob` kind's `tier`).
    pub tier: u8,
    /// Body radius (m).
    pub radius: f32,
    /// Telegraph radius (m).
    pub tele: f32,
}

/// Mob Pit rules the client draws with (generated `PIT_RULES`).
#[derive(Debug, Clone, PartialEq, GameRecord)]
pub struct PitRules {
    /// Every tier.
    pub tiers: Vec<MobTier>,
    /// `state` names by value.
    pub states: Vec<String>,
    /// Melee reach (m, from the player's centre to the mob's edge).
    pub attack_range: f32,
    /// Half angle of the swing cone (radians).
    pub attack_half_angle: f32,
    /// Swing animation length (ticks; the player's `swing` counts down from it).
    pub swing_frames: u8,
    /// Ranged reach (m).
    pub range: f32,
    /// Ticks between shots.
    pub fire_cooldown: u8,
    /// Eye height above the feet (m): first-person arrows.
    pub eye_height: f32,
    /// Hip height (m): level third-person arrows.
    pub hip_height: f32,
    /// Furthest a shot is rewound (ms).
    pub max_rewind_ms: u32,
    /// Mob body height by tier (index = tier).
    pub mob_heights: Vec<f32>,
}

/// The `PIT_RULES` constant.
pub fn pit_rules() -> PitRules {
    PitRules {
        tiers: TIERS
            .iter()
            .map(|&tier| {
                let (_, _, _, radius, tele) = tier_stats(tier);
                MobTier { tier, radius, tele }
            })
            .collect(),
        states: MOB_STATES.map(String::from).to_vec(),
        attack_range: ATTACK_RANGE,
        attack_half_angle: ATTACK_ARC_COS.acos(),
        swing_frames: SWING_FRAMES,
        range: RANGE,
        fire_cooldown: FIRE_COOLDOWN,
        eye_height: EYE_HEIGHT,
        hip_height: HIP_HEIGHT,
        max_rewind_ms: MAX_REWIND_MS as u32,
        mob_heights: (0..=4).map(mob_height).collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum MobState {
    Idle = 0,
    Chase = 1,
    Windup = 2,
    Recover = 3,
    Return = 4,
    Dead = 5,
}

struct Mob {
    id: EntityId,
    tier: u8,
    home: (f32, f32),
    x: f32,
    y: f32,
    face: f32,
    hp: i32,
    state: MobState,
    timer: u16,
    target: Option<PlayerId>,
    generation: u32,
    respawn_at: u64,
    wander: (f32, f32),
    /// The hood tier its hit points are scaled to (for the HP bar).
    difficulty: u32,
    /// The content pack's hit points (percent, for the HP bar).
    hp_pct: u16,
}

struct Player {
    id: EntityId,
    principal: String,
    name: String,
    state: MoveState,
    credits: u8,
    /// Ticks the host dropped that this player has not used yet.
    owed: u16,
    hp: i32,
    face: f32,
    swing_cd: u8,
    swing_anim: u8,
    dead_until: Option<u64>,
    level: u32,
    regen: u8,
    /// Ranged cooldown (ticks) and bow animation frames.
    fire_cd: u8,
    shot_anim: u8,
    /// Handed to a neighbouring room: no more steps here.
    leaving: bool,
    /// Told it must reload at the entrance (it runs older content).
    told_stale: bool,
}

struct PendingKill {
    principal: String,
    x: f32,
    y: f32,
    emitted: Duration,
}

/// Which room.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoomConfig {
    /// Room coordinates.
    pub room: (i32, i32),
    /// World seed.
    pub seed: u32,
}

impl RoomConfig {
    /// The room of zone `pit.<x>.<y>` in world `seed`.
    pub fn for_zone(zone_id: &str, seed: u32) -> Option<Self> {
        parse_room(zone_id).map(|room| Self { room, seed })
    }
}

/// What a room keeps while it hibernates (and in every periodic persist).
#[derive(Debug, Default, Serialize, Deserialize)]
struct Snapshot {
    kills: u32,
    mobs: Vec<SavedMob>,
    /// Where each player last stood, by verified principal: the parked
    /// players and, at the time of the snapshot, the connected ones (so a
    /// node crash keeps the last persisted position).
    #[serde(default)]
    parked: Vec<Parked>,
}

/// A player's place in a room after its slot ended (closed the page, grace
/// expired, room restarted): a fresh join of the same principal resumes
/// there instead of at a spawn point.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Parked {
    principal: String,
    x: f32,
    y: f32,
    face: f32,
    hp: i32,
}

/// Most players a room remembers (oldest forgotten first).
const MAX_PARKED: usize = 256;

#[derive(Debug, Serialize, Deserialize)]
struct SavedMob {
    id: u32,
    x: f32,
    y: f32,
    face: f32,
    hp: i32,
    dead: bool,
    generation: u32,
    /// Ticks until a dead mob respawns.
    respawn_in: u64,
}

/// The Mob Pit room zone.
pub struct MobPit {
    room: RoomConfig,
    players: BTreeMap<PlayerId, Player>,
    mobs: Vec<Mob>,
    pillars: Vec<Pillar>,
    pending: HashMap<String, PendingKill>,
    /// Facts refused by backpressure, retried each tick (oldest first).
    backlog: VecDeque<(PlayerId, Fact, f32, f32)>,
    last_tick: Option<Duration>,
    tick_ms: f32,
    tick_max_ms: f32,
    tick_max_window: f32,
    window_ticks: u32,
    committed: u32,
    kills: u32,
    restored: bool,
    /// The last tick run (hibernation stores timers relative to it).
    tick: u64,
    /// Players whose slot ended here, oldest first (see [`Parked`]).
    parked: Vec<Parked>,
    /// The tiers above the room (from their cells).
    world: WorldTiers,
    /// This activation told its borough it is awake (arms its events).
    awake_reported: bool,
    /// Ranged shots of this tick, resolved after the players stepped.
    fires: Vec<Fire>,
    /// The content pack of the room's content version.
    pack: ContentPack,
}

/// A ranged shot to resolve.
struct Fire {
    player: PlayerId,
    shot: u16,
    client_tick: u32,
    delay_ms: u16,
    origin: [f32; 3],
    dir: [f32; 3],
    compensated: bool,
}

fn spawn_state(player: PlayerId) -> MoveState {
    let (x, y) = spawn_point(player);
    MoveState {
        x,
        y,
        ..MoveState::default()
    }
}

fn spawn_point(player: PlayerId) -> (f32, f32) {
    const SPOTS: [(f32, f32); 6] = [
        (-4.5, -4.5),
        (4.5, -4.5),
        (4.5, 4.5),
        (-4.5, 4.5),
        (0.0, -5.5),
        (0.0, 5.5),
    ];
    SPOTS[player.0 as usize % SPOTS.len()]
}

fn push_out_of_pillars(x: &mut f32, y: &mut f32, radius: f32, pillars: &[Pillar]) {
    for &Pillar {
        x: px,
        y: py,
        r: pr,
        ..
    } in pillars
    {
        let (ox, oy) = (*x - px, *y - py);
        let d2 = ox * ox + oy * oy;
        let min = radius + pr;
        if d2 < min * min {
            let d = d2.sqrt().max(1e-4);
            *x = px + ox / d * min;
            *y = py + oy / d * min;
        }
    }
    let lim = movement::HALF - radius;
    *x = x.clamp(-lim, lim);
    *y = y.clamp(-lim, lim);
}

fn step_toward(x: &mut f32, y: &mut f32, tx: f32, ty: f32, stride: f32) -> f32 {
    let (dx, dy) = (tx - *x, ty - *y);
    let d = (dx * dx + dy * dy).sqrt();
    if d > 1e-4 {
        let s = stride.min(d);
        *x += dx / d * s;
        *y += dy / d * s;
    }
    dy.atan2(dx)
}

fn dist(a: (f32, f32), b: (f32, f32)) -> f32 {
    ((a.0 - b.0).powi(2) + (a.1 - b.1).powi(2)).sqrt()
}

/// Carried through a doorway: the player's [`MoveState`] in the
/// neighbour's frame (x y vx vy z vz as f32 LE, dash, cd): a jump carries on.
fn encode_carried(s: &MoveState) -> Vec<u8> {
    let mut out = Vec::with_capacity(26);
    for v in [s.x, s.y, s.vx, s.vy, s.z, s.vz] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(&[s.dash, s.cd]);
    out
}

/// Inverse of [`encode_carried`]; None unless it is a place a player can be.
fn decode_carried(bytes: &[u8]) -> Option<MoveState> {
    if bytes.len() != 26 {
        return None;
    }
    let f = |i: usize| f32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().expect("4 bytes"));
    let state = MoveState {
        x: f(0),
        y: f(1),
        vx: f(2),
        vy: f(3),
        z: f(4),
        vz: f(5),
        dash: bytes[24],
        cd: bytes[25],
    };
    admissible(&state).then(|| rooms::canonicalize(state))
}

impl MobPit {
    /// The room this zone is.
    pub fn room(&self) -> RoomConfig {
        self.room
    }

    fn profile_event(xp: u64, level: u32, kills: u32, source: &str) -> PitEvent {
        PitEvent::Profile(Profile {
            xp,
            level,
            kills,
            level_xp: level_floor(level),
            next_level_xp: level_floor(level + 1),
            source: source.into(),
        })
    }

    fn player_by_principal(&self, principal: &str) -> Option<PlayerId> {
        self.players
            .iter()
            .find(|(_, p)| p.principal == principal)
            .map(|(id, _)| *id)
    }

    /// Where `player` stands now, unless it is dead or handed off.
    fn parked_of(player: &Player) -> Option<Parked> {
        (player.dead_until.is_none() && !player.leaving && !player.principal.is_empty()).then(
            || Parked {
                principal: player.principal.clone(),
                x: player.state.x,
                y: player.state.y,
                face: player.face,
                hp: player.hp,
            },
        )
    }

    /// Remember `parked` (replacing the principal's older entry).
    fn park(&mut self, parked: Parked) {
        self.parked.retain(|p| p.principal != parked.principal);
        if self.parked.len() >= MAX_PARKED {
            self.parked.remove(0);
        }
        self.parked.push(parked);
    }

    /// Take `principal`'s parked place, if it is still a place a player can
    /// stand (never trusted blindly: snapshots outlive code changes).
    fn unpark(&mut self, principal: &str) -> Option<(MoveState, i32, f32)> {
        let index = self.parked.iter().position(|p| p.principal == principal)?;
        let parked = self.parked.remove(index);
        let state = MoveState {
            x: parked.x,
            y: parked.y,
            ..MoveState::default()
        };
        (admissible(&state) && parked.face.is_finite()).then(|| {
            (
                rooms::canonicalize(state),
                parked.hp.clamp(1, PLAYER_HP),
                parked.face,
            )
        })
    }

    fn emit_kill(&mut self, ctx: &mut Ctx<'_, Self>, killer: PlayerId, mob: usize) {
        let m = &self.mobs[mob];
        let (x, y, tier, id, generation) = (m.x, m.y, m.tier, m.id.get(), m.generation);
        let Some(player) = self.players.get(&killer) else {
            return;
        };
        let player_name = player.name.clone();
        self.kills += 1;
        // The cell command's own input: what died, never who or how much XP.
        let input = RecordKillInput {
            mob: format!("{}/m{id}", ctx.zone_id()),
            tier,
            name: player.name.clone(),
        };
        let Ok(fact) = ctx.fact_with_key(&input, &[&format_args!("m{id}"), &generation]) else {
            return;
        };
        let key = fact.key().to_string();
        ctx.broadcast(&PitEvent::Kill {
            key,
            mob: id,
            tier,
            x,
            y,
            killer: player.id.get(),
            killer_name: player.name.clone(),
        });
        self.backlog.push_back((killer, fact, x, y));
        self.flush_backlog(ctx);
        if tier == BOSS_TIER {
            self.boss_fell(ctx, generation, player_name);
        }
    }

    /// Tell the borough a room of it is awake (a zone fact, once per
    /// activation): its cell arms its world event cycle.
    fn report_awake(&mut self, ctx: &mut Ctx<'_, Self>) {
        let input = mobpit_tiers::RoomAwakeInput {
            borough: pair_shard((self.world.borough_x, self.world.borough_y)),
            room: ctx.zone_id().to_string(),
        };
        let boot = ctx.boot_epoch();
        if let Ok(fact) = ctx.fact_with_key(&input, &[&"awake", &boot]) {
            self.awake_reported = ctx.emit_fact(fact).is_ok();
        }
    }

    /// The effect of the borough's world event right now.
    fn event(&self) -> &str {
        self.world.event.as_str()
    }

    /// The hood's boss fell here: a zone fact into the hood's cell (keyed
    /// by the boss's life; the hood raises its tier and brings the boss
    /// back on its own schedule) and the news to everyone in the room.
    fn boss_fell(&mut self, ctx: &mut Ctx<'_, Self>, generation: u32, by: String) {
        let hood = (self.world.hood_x, self.world.hood_y);
        let input = BossDefeatedInput {
            hood: pair_shard(hood),
            room: ctx.zone_id().to_string(),
            generation,
            by: by.clone(),
        };
        if let Ok(fact) = ctx.fact_with_key(&input, &[&"boss", &generation]) {
            if ctx.emit_fact(fact).is_err() {
                // Backpressure or shutdown: the hood learns of the next fall.
            }
        }
        self.world.boss_down = true;
        self.world.hood_tier += 1;
        self.world.last_by = by.clone();
        ctx.broadcast(&PitEvent::BossDown {
            by,
            hood_tier: self.world.hood_tier,
        });
        ctx.broadcast(&PitEvent::World(self.world.clone()));
    }

    /// Hand queued facts to the host; never blocks. Backpressure keeps the
    /// rest for the next tick.
    fn flush_backlog(&mut self, ctx: &mut Ctx<'_, Self>) {
        while let Some((player, fact, x, y)) = self.backlog.pop_front() {
            let key = fact.key().to_string();
            match ctx.emit_fact_for(player, fact.clone()) {
                Ok(()) => {
                    let principal = self
                        .players
                        .get(&player)
                        .map(|p| p.principal.clone())
                        .unwrap_or_default();
                    self.pending.insert(
                        key,
                        PendingKill {
                            principal,
                            x,
                            y,
                            emitted: ctx.now(),
                        },
                    );
                }
                Err(EmitError::Backpressure) => {
                    self.backlog.push_front((player, fact, x, y));
                    return;
                }
                // The killer left before the fact could be queued, or the key
                // is already in flight: nothing durable to do.
                Err(_) => {}
            }
        }
    }

    fn step_players(&mut self, ctx: &mut Ctx<'_, Self>) -> Vec<(PlayerId, f32, f32, f32)> {
        let tick = ctx.tick();
        let mut swings = Vec::new();
        let mut fires = Vec::new();
        // The predicted step's context: this room (its seeded obstacles).
        let context = PitRoomContext {
            seed: self.room.seed,
            x: self.room.room.0,
            y: self.room.room.1,
        };
        let blessing = self.world.event == "blessing";
        // Inputs are paced by wall-clock time: a tick the host dropped is
        // owed too, or the inputs sent during it stay queued for good (the
        // ghost then trails by the whole queue). Owed ticks are banked until
        // inputs arrive: after a stall the socket's inputs often land after
        // the catch-up tick.
        let dropped = u16::try_from(ctx.dropped_ticks()).unwrap_or(u16::MAX);
        for (id, p) in &mut self.players {
            p.credits = (p.credits + 1).min(MAX_CREDITS);
            p.owed = p.owed.saturating_add(dropped).min(MAX_OWED);
            p.swing_cd = p.swing_cd.saturating_sub(1);
            p.fire_cd = p.fire_cd.saturating_sub(1);
            p.shot_anim = p.shot_anim.saturating_sub(1);
            p.swing_anim = p.swing_anim.saturating_sub(1);
            if let Some(until) = p.dead_until {
                // Dead: inputs are consumed but ignored.
                let _ = ctx.take_inputs(*id, ConsumePolicy::Drain);
                if tick >= until {
                    let (x, y) = spawn_point(*id);
                    p.state = MoveState {
                        x,
                        y,
                        ..MoveState::default()
                    };
                    p.hp = PLAYER_HP;
                    p.dead_until = None;
                }
                continue;
            }
            if p.leaving {
                continue;
            }
            while p.credits > 0 || p.owed > 0 {
                let Some(input) = ctx.take_inputs(*id, ConsumePolicy::Next).pop() else {
                    break;
                };
                let client_tick = input.client_tick;
                let input = input.input;
                // The declared predicted step: what the client runs as WASM.
                p.state = pit_step(&p.state, &input, &context, movement::DT);
                if p.credits > 0 {
                    p.credits -= 1;
                } else {
                    p.owed -= 1;
                }
                let (sx, sy) = (f32::from(input.mx), f32::from(input.my));
                if input.aims() && input.yaw.is_finite() {
                    // First person: you face where you look.
                    p.face = input.yaw;
                } else if sx * sx + sy * sy > 20.0 * 20.0 {
                    p.face = sy.atan2(sx);
                }
                if input.fire() && p.fire_cd == 0 {
                    p.fire_cd = FIRE_COOLDOWN;
                    p.shot_anim = SHOT_FRAMES;
                    fires.push(Fire {
                        player: *id,
                        shot: input.shot,
                        client_tick,
                        delay_ms: input.delay_ms,
                        // First person: from the eye along the look; else
                        // level, at waist height, along the yaw.
                        origin: [
                            p.state.x,
                            p.state.y,
                            p.state.z + if input.aims() { EYE_HEIGHT } else { HIP_HEIGHT },
                        ],
                        dir: if input.aims() {
                            input.aim_dir()
                        } else {
                            PitInput {
                                pitch: 0.0,
                                ..input
                            }
                            .aim_dir()
                        },
                        compensated: input.buttons & PitInput::NO_COMP == 0,
                    });
                }
                if input.attack() && p.swing_cd == 0 {
                    p.swing_cd = SWING_COOLDOWN;
                    p.swing_anim = SWING_FRAMES;
                    swings.push((*id, p.state.x, p.state.y, p.face));
                }
                // Past a wall line: the neighbour takes the rest.
                if crossing(p.state.x, p.state.y).is_some() {
                    break;
                }
            }
            p.regen = p.regen.wrapping_add(1);
            // A blessing (the borough's world event) heals five times as fast.
            let every = if blessing { 3 } else { 15 };
            if p.regen % every == 0 && p.hp < PLAYER_HP {
                p.hp += 1;
            }
            ctx.set_view(*id, View::new(p.state.x, p.state.y, VIEW_RADIUS));
        }
        self.fires = fires;
        swings
    }

    /// Players past a wall line (through a doorway passage) move to the
    /// neighbouring room, at the same world point.
    fn walk_through_doorways(&mut self, ctx: &mut Ctx<'_, Self>) {
        let (rx, ry) = self.room.room;
        let crossing_now: Vec<(PlayerId, (i32, i32))> = self
            .players
            .iter()
            .filter(|(_, p)| p.dead_until.is_none() && !p.leaving)
            .filter_map(|(id, p)| Some((*id, crossing(p.state.x, p.state.y)?)))
            .collect();
        for (player, (dx, dy)) in crossing_now {
            let target = room_zone((rx + dx, ry + dy));
            // A client on older content (a live update happened) only
            // passes into the entrance, which reloads it.
            let held = ctx.is_stale(player) && target != GATE_ROOM;
            let p = self.players.get_mut(&player).expect("player");
            let carried = encode_carried(&into_neighbour((dx, dy), p.state));
            if held && !p.told_stale {
                p.told_stale = true;
                ctx.send(
                    player,
                    &PitEvent::Stale {
                        gate: GATE_ROOM.into(),
                        version: ctx.content_version().to_string(),
                    },
                );
            }
            if !held
                && parse_room(&target).is_some()
                && ctx.handoff(player, &target, carried).is_ok()
            {
                p.leaving = true;
            } else {
                // The edge of the world: back inside the wall line.
                let line = movement::HALF - PLAYER_RADIUS;
                p.state.x = p.state.x.clamp(-line, line);
                p.state.y = p.state.y.clamp(-line, line);
                p.state.vx = 0.0;
                p.state.vy = 0.0;
            }
        }
    }

    fn resolve_swings(&mut self, ctx: &mut Ctx<'_, Self>, swings: Vec<(PlayerId, f32, f32, f32)>) {
        for (player, x, y, face) in swings {
            let (fx, fy) = (face.cos(), face.sin());
            for i in 0..self.mobs.len() {
                let m = &mut self.mobs[i];
                if m.state == MobState::Dead {
                    continue;
                }
                let (_, _, _, radius, _) = tier_stats(m.tier);
                let (dx, dy) = (m.x - x, m.y - y);
                let d = (dx * dx + dy * dy).sqrt();
                if d > ATTACK_RANGE + radius {
                    continue;
                }
                let cos = if d < 1e-3 {
                    1.0
                } else {
                    (dx * fx + dy * fy) / d
                };
                if cos < ATTACK_ARC_COS && d > radius + PLAYER_RADIUS {
                    continue;
                }
                self.hurt_mob(ctx, player, i, ATTACK_DAMAGE);
            }
        }
    }

    /// `player` deals `damage` to mob `i` (it turns on them; at 0 HP it
    /// dies and the kill is a fact).
    fn hurt_mob(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId, i: usize, damage: i32) {
        let m = &mut self.mobs[i];
        m.hp -= damage;
        if matches!(m.state, MobState::Idle | MobState::Return) {
            m.state = MobState::Chase;
            m.target = Some(player);
        }
        if m.hp <= 0 {
            m.hp = 0;
            m.state = MobState::Dead;
            m.respawn_at = ctx.tick()
                + if m.tier == BOSS_TIER {
                    BOSS_RESPAWN_TICKS
                } else {
                    RESPAWN_TICKS
                };
            m.target = None;
            self.emit_kill(ctx, player, i);
        }
    }

    /// Resolve this tick's ranged shots, **lag-compensated**: each is a ray
    /// from the shooter's eye (its jump height included) along its aim,
    /// tested against the mobs as they were at the shooter's view time
    /// (`client_tick` minus the interpolation delay the shot carries,
    /// `ctx.rewind()`, clamped to `MAX_REWIND_MS`), or against the latest
    /// state with compensation off. Obstacles stop arrows. Each shooter
    /// gets a verdict ([`PitEvent::Shot`]).
    fn resolve_fires(&mut self, ctx: &mut Ctx<'_, Self>) {
        let fires = std::mem::take(&mut self.fires);
        let mut verdicts = Vec::with_capacity(fires.len());
        {
            let rewind = ctx.rewind();
            let Some(latest) = rewind.latest().map(|f| f.time()) else {
                return;
            };
            for fire in &fires {
                let query = HitQuery::ray(fire.origin, fire.dir, RANGE);
                let timing = ShotTiming {
                    client_tick: fire.client_tick,
                    interp_delay: Duration::from_millis(fire.delay_ms.into()),
                };
                let resolved = rewind.resolve(timing);
                let (time, age, requested, clamped) = match (fire.compensated, resolved) {
                    (true, Some(r)) => (r.time, r.age, r.requested_age, r.clamped),
                    (_, r) => (
                        latest,
                        Duration::ZERO,
                        r.map_or(Duration::ZERO, |r| r.requested_age),
                        false,
                    ),
                };
                let mobs = &self.mobs;
                let hit = rewind
                    .hit_test(time, &query, |id| {
                        mobs.get(id as usize - 1)
                            .is_some_and(|m| m.state != MobState::Dead)
                    })
                    .map(|h| (h.id, h.distance))
                    .filter(|&(_, d)| d < self.blocked_at(fire.origin, fire.dir));
                verdicts.push((hit, time, age, requested, clamped));
            }
        }
        for (fire, (hit, _, age, requested, clamped)) in fires.into_iter().zip(verdicts) {
            let distance = hit.map_or(RANGE.min(self.blocked_at(fire.origin, fire.dir)), |h| h.1);
            let at = [
                fire.origin[0] + fire.dir[0] * distance,
                fire.origin[1] + fire.dir[1] * distance,
                fire.origin[2] + fire.dir[2] * distance,
            ];
            if let Some((id, _)) = hit {
                self.hurt_mob(ctx, fire.player, id as usize - 1, RANGED_DAMAGE);
            }
            ctx.send(
                fire.player,
                &PitEvent::Shot {
                    shot: fire.shot,
                    hit: hit.map(|h| h.0),
                    compensated: fire.compensated,
                    age_ms: age.as_secs_f32() * 1000.0,
                    requested_age_ms: requested.as_secs_f32() * 1000.0,
                    clamped: fire.compensated && clamped,
                    x: at[0],
                    y: at[1],
                    z: at[2],
                },
            );
        }
    }

    /// Distance along a ray to the first obstacle it meets below its top
    /// (a column, boulder or tree), or the range.
    fn blocked_at(&self, origin: [f32; 3], dir: [f32; 3]) -> f32 {
        let flat = (dir[0] * dir[0] + dir[1] * dir[1]).sqrt();
        if flat < 1e-4 {
            return RANGE;
        }
        let (ux, uy) = (dir[0] / flat, dir[1] / flat);
        let mut best = RANGE;
        for o in &self.pillars {
            let (ox, oy) = (o.x - origin[0], o.y - origin[1]);
            let along = ox * ux + oy * uy;
            let off2 = ox * ox + oy * oy - along * along;
            if along <= 0.0 || off2 > o.r * o.r {
                continue;
            }
            // Horizontal distance to the entry, then along the 3D ray.
            let entry = (along - (o.r * o.r - off2).sqrt()) / flat;
            if (0.0..best).contains(&entry) && origin[2] + dir[2] * entry < OBSTACLE_HEIGHT {
                best = entry;
            }
        }
        best
    }

    fn step_mobs(&mut self, ctx: &mut Ctx<'_, Self>) {
        let dt = ctx.dt_secs();
        let tick = ctx.tick();
        let living: Vec<(PlayerId, f32, f32)> = self
            .players
            .iter()
            .filter(|(_, p)| p.dead_until.is_none())
            .map(|(id, p)| (*id, p.state.x, p.state.y))
            .collect();
        // Who a slam reaches: the ring is on the floor, a jump clears it.
        let low: Vec<PlayerId> = self
            .players
            .iter()
            .filter(|(_, p)| p.state.z < SLAM_CLEARANCE)
            .map(|(id, _)| *id)
            .collect();
        let mut hits: Vec<(PlayerId, i32)> = Vec::new();
        let difficulty = self.world.hood_tier;
        let boss_down = self.world.boss_down;
        // The borough's world event: an invasion (mobs aggro from twice as
        // far and come back at once) or a frenzy (mobs 40 % faster).
        let invasion = self.event() == "invasion";
        let pace = if self.event() == "frenzy" { 1.4 } else { 1.0 } * self.pack.mob_pace;
        let aggro = if invasion { AGGRO * 2.0 } else { AGGRO };
        for i in 0..self.mobs.len() {
            let tier = self.mobs[i].tier;
            let (_, speed, _, radius, tele) = tier_stats(tier);
            let (max_hp, damage) = (
                pack_hp(tier, difficulty, &self.pack),
                damage(tier, difficulty),
            );
            let target_pos = |t: Option<PlayerId>| {
                t.and_then(|t| living.iter().find(|(id, _, _)| *id == t))
                    .map(|(_, x, y)| (*x, *y))
            };
            let m = &mut self.mobs[i];
            m.difficulty = difficulty;
            m.hp_pct = self.pack.mob_hp_pct;
            match m.state {
                MobState::Dead => {
                    // A boss returns only once its hood says so.
                    // An invasion brings the fallen back after a second.
                    let due = tick >= m.respawn_at
                        || (invasion
                            && m.tier != BOSS_TIER
                            && tick + RESPAWN_TICKS >= m.respawn_at + 30);
                    if due && !(m.tier == BOSS_TIER && boss_down) {
                        m.generation += 1;
                        m.hp = max_hp;
                        (m.x, m.y) = m.home;
                        m.state = MobState::Idle;
                    }
                    continue;
                }
                MobState::Idle => {
                    let nearest = living
                        .iter()
                        .map(|(id, x, y)| (*id, dist((m.x, m.y), (*x, *y))))
                        .filter(|(_, d)| *d < aggro)
                        .min_by(|a, b| a.1.total_cmp(&b.1));
                    if let Some((id, _)) = nearest {
                        m.state = MobState::Chase;
                        m.target = Some(id);
                    } else if dist((m.x, m.y), m.wander) < 0.3 {
                        let a = ctx.rng().next_f32() * std::f32::consts::TAU;
                        let r = ctx.rng().next_f32() * 2.5;
                        let m = &mut self.mobs[i];
                        m.wander = (m.home.0 + r * a.cos(), m.home.1 + r * a.sin());
                    } else {
                        let (wx, wy) = m.wander;
                        m.face = step_toward(&mut m.x, &mut m.y, wx, wy, 1.0 * dt);
                    }
                }
                MobState::Chase => match target_pos(m.target) {
                    Some((tx, ty)) if dist(m.home, (m.x, m.y)) < LEASH => {
                        let reach = radius + PLAYER_RADIUS + 0.6;
                        if dist((m.x, m.y), (tx, ty)) <= reach {
                            m.state = MobState::Windup;
                            m.timer = WINDUP_TICKS;
                            m.face = (ty - m.y).atan2(tx - m.x);
                        } else {
                            m.face = step_toward(&mut m.x, &mut m.y, tx, ty, speed * pace * dt);
                        }
                    }
                    _ => {
                        m.state = MobState::Return;
                        m.target = None;
                    }
                },
                MobState::Windup => {
                    m.timer = m.timer.saturating_sub(1);
                    if m.timer == 0 {
                        for (id, x, y) in &living {
                            if dist((m.x, m.y), (*x, *y)) <= tele + PLAYER_RADIUS * 0.5
                                && low.contains(id)
                            {
                                hits.push((*id, damage));
                            }
                        }
                        m.state = MobState::Recover;
                        m.timer = RECOVER_TICKS;
                    }
                }
                MobState::Recover => {
                    m.timer = m.timer.saturating_sub(1);
                    if m.timer == 0 {
                        m.state = MobState::Chase;
                    }
                }
                MobState::Return => {
                    let (hx, hy) = m.home;
                    m.face = step_toward(&mut m.x, &mut m.y, hx, hy, speed * 1.3 * dt);
                    if m.tier != BOSS_TIER {
                        m.hp = (m.hp + 1).min(max_hp);
                    }
                    if dist((m.x, m.y), m.home) < 0.3 {
                        m.state = MobState::Idle;
                        m.wander = m.home;
                    }
                }
            }
            let m = &mut self.mobs[i];
            push_out_of_pillars(&mut m.x, &mut m.y, radius, &self.pillars);
        }
        // Light separation so a pack does not stack on one spot.
        for i in 0..self.mobs.len() {
            for j in (i + 1)..self.mobs.len() {
                let (a, b) = (&self.mobs[i], &self.mobs[j]);
                if a.state == MobState::Dead || b.state == MobState::Dead {
                    continue;
                }
                let min = tier_stats(a.tier).3 + tier_stats(b.tier).3;
                let (dx, dy) = (b.x - a.x, b.y - a.y);
                let d = (dx * dx + dy * dy).sqrt();
                if d < min && d > 1e-4 {
                    let push = (min - d) * 0.5;
                    let (nx, ny) = (dx / d * push, dy / d * push);
                    self.mobs[i].x -= nx;
                    self.mobs[i].y -= ny;
                    self.mobs[j].x += nx;
                    self.mobs[j].y += ny;
                }
            }
        }
        for (id, damage) in hits {
            let Some(p) = self.players.get_mut(&id) else {
                continue;
            };
            if p.dead_until.is_some() {
                continue;
            }
            p.hp -= damage;
            if p.hp <= 0 {
                p.hp = 0;
                p.state.vx = 0.0;
                p.state.vy = 0.0;
                p.dead_until = Some(tick + PLAYER_RESPAWN_TICKS);
                for m in &mut self.mobs {
                    if m.target == Some(id) {
                        m.target = None;
                    }
                }
                ctx.send(
                    id,
                    &PitEvent::Died {
                        respawn_ms: (PLAYER_RESPAWN_TICKS * 1000 / 30) as u32,
                    },
                );
            }
        }
    }

    fn measure_tick(&mut self, now: Duration) {
        if let Some(last) = self.last_tick {
            let ms = now.saturating_sub(last).as_secs_f32() * 1000.0;
            self.tick_ms = if self.tick_ms == 0.0 {
                ms
            } else {
                self.tick_ms * 0.9 + ms * 0.1
            };
            self.tick_max_window = self.tick_max_window.max(ms);
            self.window_ticks += 1;
            if self.window_ticks >= 30 {
                self.tick_max_ms = self.tick_max_window;
                self.tick_max_window = 0.0;
                self.window_ticks = 0;
            }
        }
        self.last_tick = Some(now);
    }

    /// Admit `player` at `state` (with `hp` and facing `face`) and greet it.
    fn admit(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        state: MoveState,
        hp: i32,
        face: f32,
    ) {
        let (principal, name) = ctx
            .player(player)
            .map(|p| {
                (
                    p.principal.clone(),
                    p.name.clone().unwrap_or_else(|| p.principal.clone()),
                )
            })
            .unwrap_or_default();
        let entity = EntityId(1000 + player.0);
        self.players.insert(
            player,
            Player {
                id: entity,
                principal,
                name,
                state,
                credits: MAX_CREDITS,
                owed: 0,
                hp,
                face,
                swing_cd: 0,
                swing_anim: 0,
                dead_until: None,
                level: 1,
                regen: 0,
                fire_cd: 0,
                shot_anim: 0,
                leaving: false,
                told_stale: false,
            },
        );
        self.greet(ctx, player);
    }

    /// Tell `player`'s client session who it is and where (on every join,
    /// and again on a rejoin: a reloaded page knows nothing).
    fn greet(&self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let Some(p) = self.players.get(&player) else {
            return;
        };
        let (entity, state) = (p.id, p.state);
        ctx.set_view(player, View::new(state.x, state.y, VIEW_RADIUS));
        ctx.send(
            player,
            &PitEvent::Hello {
                entity: entity.get(),
                half: movement::HALF,
                attack_range: ATTACK_RANGE,
                attack_arc_cos: ATTACK_ARC_COS,
                room_x: self.room.room.0,
                room_y: self.room.room.1,
                seed: self.room.seed,
                restored: self.restored,
                room_kills: self.kills,
                view_radius: VIEW_RADIUS,
            },
        );
        ctx.send(player, &PitEvent::World(self.world.clone()));
    }
}

impl Zone for MobPit {
    type Config = RoomConfig;
    type Input = PitInput;
    type Timer = ();
    type Message = PitMessage;

    fn schema() -> EntitySchema {
        schema![PlayerView, MobView, PillarView, PitView]
    }

    fn entities() -> Entities<Self> {
        Entities::new()
            .each(|z: &Self| &z.players)
            .each(|z: &Self| &z.mobs)
            .each(|z: &Self| &z.pillars)
            .one(|z: &Self| z)
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, room: RoomConfig) -> Self {
        let layout = pit_room(&PitRoomContext {
            seed: room.seed,
            x: room.room.0,
            y: room.room.1,
        });
        let mobs = layout
            .mobs
            .iter()
            .enumerate()
            .map(|(i, slot)| {
                let home = (slot.x, slot.y);
                Mob {
                    id: EntityId(i as u32 + 1),
                    tier: slot.tier,
                    home,
                    x: home.0,
                    y: home.1,
                    face: 0.0,
                    hp: pack_hp(slot.tier, 0, &content_pack(ctx.content_version())),
                    state: MobState::Idle,
                    timer: 0,
                    target: None,
                    generation: 0,
                    respawn_at: 0,
                    wander: home,
                    difficulty: 0,
                    hp_pct: content_pack(ctx.content_version()).mob_hp_pct,
                }
            })
            .collect();
        Self {
            room,
            players: BTreeMap::new(),
            mobs,
            pillars: Pillar::of(&layout.obstacles),
            pending: HashMap::new(),
            backlog: VecDeque::new(),
            last_tick: None,
            tick_ms: 0.0,
            tick_max_ms: 0.0,
            tick_max_window: 0.0,
            window_ticks: 0,
            committed: 0,
            kills: 0,
            restored: false,
            tick: 0,
            parked: Vec::new(),
            world: {
                let hood = hood_of(room.room);
                let borough = borough_of(hood);
                WorldTiers {
                    hood_x: hood.0,
                    hood_y: hood.1,
                    borough_x: borough.0,
                    borough_y: borough.1,
                    ..WorldTiers::default()
                }
            },
            awake_reported: false,
            fires: Vec::new(),
            pack: content_pack(ctx.content_version()),
        }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        // Back where it last stood here (see `Parked`), else a spawn point.
        let principal = ctx
            .player(player)
            .map(|p| p.principal.clone())
            .unwrap_or_default();
        let (state, hp, face) = self
            .unpark(&principal)
            .unwrap_or_else(|| (spawn_state(player), PLAYER_HP, 0.0));
        self.admit(ctx, player, state, hp, face);
    }

    /// A reloaded page (a fresh ticket for a slot this principal still
    /// holds): the same body, only the new client session is greeted.
    fn on_rejoin(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let name = ctx.player(player).and_then(|p| p.name.clone());
        match self.players.get_mut(&player) {
            Some(p) if !p.leaving => {
                if let Some(name) = name {
                    p.name = name;
                }
                p.swing_anim = 0;
                self.greet(ctx, player);
            }
            _ => {
                self.on_leave(ctx, player, LeaveReason::Rejoined);
                self.on_join(ctx, player);
            }
        }
    }

    fn on_handoff_join(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        _from_zone: &str,
        carried: Vec<u8>,
    ) {
        let at = decode_carried(&carried).unwrap_or_else(|| spawn_state(player));
        // Still here inside its reconnect grace (a doorway bounce): replace.
        let hp = self.players.remove(&player).map_or(PLAYER_HP, |p| p.hp);
        if let Some(principal) = ctx.player(player).map(|p| p.principal.clone()) {
            // It arrives from elsewhere: an old place here is stale.
            self.parked.retain(|p| p.principal != principal);
        }
        self.admit(ctx, player, at, hp, 0.0);
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, reason: LeaveReason) {
        let left = self.players.remove(&player);
        // Its place is kept unless it moved on (a doorway) or is rejoining.
        if !matches!(reason, LeaveReason::Handoff | LeaveReason::Rejoined) {
            if let Some(parked) = left.as_ref().and_then(Self::parked_of) {
                self.park(parked);
            }
        }
        for m in &mut self.mobs {
            if m.target == Some(player) {
                m.target = None;
            }
        }
    }

    /// The entrance is the safe room of a live content update: a client on
    /// older content is let in there (and only there) and reloaded.
    fn is_gate(&self) -> bool {
        self.room.room == (0, 0)
    }

    /// A live content update: the room runs the new pack from this tick
    /// (facts and stamps carry the new version).
    fn on_content_version_changed(&mut self, _ctx: &mut Ctx<'_, Self>, _from: &str, to: &str) {
        self.pack = content_pack(to);
        for p in self.players.values_mut() {
            p.told_stale = false;
        }
    }

    /// A watcher (a player next door, a board page): the whole room, until
    /// it says where it stands (`on_observer_focus`).
    fn on_observe(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId) {
        ctx.set_view(observer, View::new(0.0, 0.0, WATCH_RADIUS));
    }

    /// A player next door, at `(x, y)` in this room's coordinates, sees this
    /// room as it sees its own: [`VIEW_RADIUS`] around it, so interest and
    /// update rates carry on across the wall.
    fn on_observer_focus(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId, x: f32, y: f32) {
        if !(x.is_finite() && y.is_finite()) {
            return;
        }
        // A watcher stands at most one room away.
        let reach = 3.0 * movement::HALF;
        let view = View::new(x.clamp(-reach, reach), y.clamp(-reach, reach), VIEW_RADIUS);
        ctx.set_view(observer, view);
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<PitInput> {
        PitInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        self.tick = ctx.tick();
        if !self.awake_reported {
            self.report_awake(ctx);
        }
        self.measure_tick(ctx.now());
        let swings = self.step_players(ctx);
        // Shots first, against the history up to the previous tick (what
        // the shooters were shown), then the melee swings.
        self.resolve_fires(ctx);
        self.resolve_swings(ctx, swings);
        self.step_mobs(ctx);
        self.walk_through_doorways(ctx);
        self.flush_backlog(ctx);
    }

    fn on_message(&mut self, ctx: &mut Ctx<'_, Self>, message: PitMessage) {
        // One tier pushed: the room's world with that tier replaced.
        let message = match message {
            PitMessage::Hood {
                tier,
                boss_down,
                last_by,
            } => PitMessage::World(WorldTiers {
                hood_tier: tier,
                boss_down,
                last_by,
                ..self.world.clone()
            }),
            PitMessage::Borough {
                bosses_down,
                weather,
                event,
                event_until_ms,
                hoods_down,
            } => PitMessage::World(WorldTiers {
                borough_bosses_down: bosses_down,
                weather,
                event,
                event_until_ms,
                hoods_down,
                ..self.world.clone()
            }),
            PitMessage::City {
                bosses_down,
                banner,
            } => PitMessage::World(WorldTiers {
                city_bosses_down: bosses_down,
                banner,
                ..self.world.clone()
            }),
            message => message,
        };
        match message {
            PitMessage::Loaded {
                principal,
                xp,
                level,
                kills,
            } => {
                let Some(id) = self.player_by_principal(&principal) else {
                    return;
                };
                if let Some(p) = self.players.get_mut(&id) {
                    p.level = level.max(1);
                }
                ctx.send(id, &Self::profile_event(xp, level.max(1), kills, "cell"));
            }
            // Mapped to `World` above.
            PitMessage::Hood { .. } | PitMessage::Borough { .. } | PitMessage::City { .. } => {}
            message @ (PitMessage::World(_) | PitMessage::WorldKeepCity(_)) => {
                let (world, keep_city) = match message {
                    PitMessage::World(world) => (world, false),
                    PitMessage::WorldKeepCity(world) => (world, true),
                    _ => unreachable!(),
                };
                // Only the hood, borough and city this room belongs to.
                // A fall reported here may not have reached the hood yet:
                // tiers never go down, and a boss that died here stays down
                // until its time is up.
                let tick = self.tick;
                let fallen_here = self.mobs.iter().any(|m| {
                    m.tier == BOSS_TIER && m.state == MobState::Dead && tick < m.respawn_at
                });
                let world = WorldTiers {
                    hood_x: self.world.hood_x,
                    hood_y: self.world.hood_y,
                    borough_x: self.world.borough_x,
                    borough_y: self.world.borough_y,
                    hood_tier: world.hood_tier.max(self.world.hood_tier),
                    boss_down: world.boss_down || fallen_here,
                    ..world
                };
                let world = if keep_city {
                    WorldTiers {
                        city_bosses_down: self.world.city_bosses_down,
                        banner: self.world.banner.clone(),
                        ..world
                    }
                } else {
                    world
                };
                if world != self.world {
                    self.world = world;
                    ctx.broadcast(&PitEvent::World(self.world.clone()));
                }
            }
        }
    }

    fn on_fact_settled(&mut self, ctx: &mut Ctx<'_, Self>, settlement: FactSettlement) {
        // The host's own lifecycle facts (presence) are not kills.
        if distributed_game::lifecycle::LIFECYCLE_COMMANDS.contains(&settlement.command.as_str()) {
            return;
        }
        let pending = self.pending.remove(&settlement.key);
        let now = ctx.now();
        let (principal, x, y, waited_ms) = match &pending {
            Some(p) => (
                p.principal.clone(),
                p.x,
                p.y,
                now.saturating_sub(p.emitted)
                    .as_millis()
                    .min(u128::from(u32::MAX)) as u32,
            ),
            None => (String::new(), 0.0, 0.0, 0),
        };
        let player = self.player_by_principal(&principal);
        match settlement.outcome {
            FactOutcome::Committed(receipt) => {
                self.committed += 1;
                let Ok(kill) = receipt.payload_as::<KillReceipt>() else {
                    return;
                };
                let Some(player) = player else { return };
                if let Some(p) = self.players.get_mut(&player) {
                    p.level = kill.level;
                }
                ctx.send(
                    player,
                    &PitEvent::Saved {
                        key: settlement.key,
                        ok: true,
                        replayed: receipt.replayed,
                        xp: kill.xp,
                        total_xp: kill.total_xp,
                        kills: kill.kills,
                        level: kill.level,
                        waited_ms,
                        x,
                        y,
                        error: None,
                        attempts: 0,
                    },
                );
                ctx.send(
                    player,
                    &Self::profile_event(kill.total_xp, kill.level, kill.kills, "settled"),
                );
            }
            FactOutcome::Failed { error, attempts } => {
                if let Some(player) = player {
                    ctx.send(
                        player,
                        &PitEvent::Saved {
                            key: settlement.key,
                            ok: false,
                            replayed: false,
                            xp: 0,
                            total_xp: 0,
                            kills: 0,
                            level: 0,
                            waited_ms,
                            x,
                            y,
                            error: Some(format!("{error:?}")),
                            attempts,
                        },
                    );
                }
            }
        }
    }

    /// The mobs' hit capsules each tick (ranged lag compensation).
    fn record_rewind(&self, frame: &mut RewindFrame) {
        for m in self.mobs.iter().filter(|m| m.state != MobState::Dead) {
            let half = mob_height(m.tier) / 2.0;
            frame.push(Historic::capsule(
                m.id.get(),
                [m.x, m.y, half],
                half,
                tier_stats(m.tier).3,
            ));
        }
    }

    fn has_pending_work(&self) -> bool {
        !self.backlog.is_empty()
    }

    fn hibernate(&self) -> Option<Vec<u8>> {
        // Called between ticks; timers are relative to the last tick.
        let tick = self.tick;
        let snapshot = Snapshot {
            kills: self.kills,
            mobs: self
                .mobs
                .iter()
                .map(|m| SavedMob {
                    id: m.id.get(),
                    x: m.x,
                    y: m.y,
                    face: m.face,
                    hp: m.hp,
                    dead: m.state == MobState::Dead,
                    generation: m.generation,
                    respawn_in: m.respawn_at.saturating_sub(tick),
                })
                .collect(),
            parked: {
                // Connected players too, newest: a crash after this
                // snapshot brings them back here.
                let mut parked = self.parked.clone();
                for p in self.players.values().filter_map(Self::parked_of) {
                    parked.retain(|q| q.principal != p.principal);
                    parked.push(p);
                }
                let excess = parked.len().saturating_sub(MAX_PARKED);
                parked.drain(..excess);
                parked
            },
        };
        serde_json::to_vec(&snapshot).ok()
    }

    fn on_restore(&mut self, ctx: &mut Ctx<'_, Self>, state: &[u8], _saved_version: &str) {
        let Ok(snapshot) = serde_json::from_slice::<Snapshot>(state) else {
            return;
        };
        self.kills = snapshot.kills;
        self.restored = true;
        self.parked = snapshot.parked;
        let tick = ctx.tick();
        for saved in snapshot.mobs {
            let Some(m) = self.mobs.iter_mut().find(|m| m.id.get() == saved.id) else {
                continue;
            };
            m.generation = saved.generation;
            m.face = saved.face;
            if saved.dead {
                m.state = MobState::Dead;
                m.hp = 0;
                m.respawn_at = tick + saved.respawn_in;
            } else {
                // Back home-side where it stood, calm again.
                m.x = saved.x;
                m.y = saved.y;
                m.hp = saved
                    .hp
                    .clamp(1, pack_hp(m.tier, mobpit_tiers::MAX_TIER, &self.pack));
                m.state = MobState::Return;
            }
        }
    }
}

impl Entity for Player {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Project<PlayerView> for Player {
    fn view(&self) -> Option<PlayerView> {
        Some(PlayerView::project(
            self,
            PlayerViewRest {
                hp: self.hp.clamp(0, 255) as u8,
                swing: self.swing_anim,
                level: self.level.min(u32::from(u16::MAX)) as u16,
                dead: self.dead_until.is_some(),
                shot: self.shot_anim,
            },
        ))
    }
}

impl Entity for Mob {
    fn entity_id(&self) -> EntityId {
        self.id
    }
}

impl Project<MobView> for Mob {
    fn view(&self) -> Option<MobView> {
        if self.state == MobState::Dead {
            return None;
        }
        let max_hp = (max_hp(self.tier, self.difficulty) * i32::from(self.hp_pct) / 100).max(1);
        let tele = if self.state == MobState::Windup {
            255 - (u32::from(self.timer) * 255 / u32::from(WINDUP_TICKS))
        } else {
            0
        };
        Some(MobView::project(
            self,
            MobViewRest {
                hp: (self.hp.max(0) * 100 / max_hp).min(100) as u8,
                state: self.state as u8,
                tele: tele as u8,
            },
        ))
    }
}

impl Entity for MobPit {
    fn entity_id(&self) -> EntityId {
        PIT_ENTITY
    }
}

impl Project<PitView> for MobPit {
    fn view(&self) -> Option<PitView> {
        let clamp16 = |n: usize| n.min(65_535) as u16;
        Some(PitView::project(
            self,
            PitViewRest {
                pending: clamp16(self.pending.len()),
                committed: self.committed,
                backlog: clamp16(self.backlog.len()),
                tick_ms: self.tick_ms,
                tick_max_ms: self.tick_max_ms,
                kills: self.kills,
            },
        ))
    }
}

/// Room numbers for tests and tooling.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PitStats {
    /// Facts emitted and not yet settled (incl. backpressure backlog).
    pub pending: usize,
    /// Tick interval EMA (ms).
    pub tick_ms: f32,
    /// Longest tick interval of the last 30 ticks (ms).
    pub tick_max_ms: f32,
    /// Settlements received.
    pub committed: u32,
    /// Living mobs.
    pub mobs_alive: usize,
    /// Players.
    pub players: usize,
    /// Mobs killed in the room.
    pub kills: u32,
    /// The room came back from a snapshot.
    pub restored: bool,
}

impl MobPit {
    /// Current numbers.
    /// The content pack the room runs.
    pub fn pack(&self) -> &ContentPack {
        &self.pack
    }

    pub fn stats(&self) -> PitStats {
        PitStats {
            pending: self.pending.len() + self.backlog.len(),
            tick_ms: self.tick_ms,
            tick_max_ms: self.tick_max_ms,
            committed: self.committed,
            mobs_alive: self
                .mobs
                .iter()
                .filter(|m| m.state != MobState::Dead)
                .count(),
            players: self.players.len(),
            kills: self.kills,
            restored: self.restored,
        }
    }

    /// Mob positions `(id, x, y, alive)` (tests and bots).
    pub fn mob_positions(&self) -> Vec<(u32, f32, f32, bool)> {
        self.mobs
            .iter()
            .map(|m| (m.id.get(), m.x, m.y, m.state != MobState::Dead))
            .collect()
    }

    /// A player's position (tests and bots).
    pub fn player_position(&self, player: PlayerId) -> Option<(f32, f32)> {
        self.players.get(&player).map(|p| (p.state.x, p.state.y))
    }

    /// A player's predicted state and hit points (tests and bots).
    pub fn player_state(&self, player: PlayerId) -> Option<(MoveState, i32)> {
        self.players.get(&player).map(|p| (p.state, p.hp))
    }

    /// Mobs winding up a slam: `(id, x, y, ticks left)` (tests and bots).
    pub fn mob_windups(&self) -> Vec<(u32, f32, f32, u16)> {
        self.mobs
            .iter()
            .filter(|m| m.state == MobState::Windup)
            .map(|m| (m.id.get(), m.x, m.y, m.timer))
            .collect()
    }
}

/// Bot payload: the direction as a move plus attack always held.
pub fn encode_bot_input(dir: [f32; 2]) -> Vec<u8> {
    let axis = |v: f32| (v.clamp(-1.0, 1.0) * 127.0).round() as i8;
    PitInput {
        mx: axis(dir[0]),
        my: axis(dir[1]),
        buttons: PitInput::ATTACK,
        ..PitInput::default()
    }
    .to_payload()
}
