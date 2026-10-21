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

use distributed_game::{
    schema, ConsumePolicy, Ctx, EmitError, Entities, Entity, EntityId, EntitySchema, Fact,
    FactOutcome, FactSettlement, GameEvent, GameInput, GameRecord, GameSurface, LeaveReason,
    ObserverId, PlayerId, Project, Replicated, View, Zone,
};
use game_sim::arena::{self as movement, MoveState, PILLARS, PLAYER_RADIUS};
use game_sim::mobpit::{
    self as rooms, admissible, crossing, into_neighbour, parse_room, pit_room, pit_world,
    room_zone, PitRoomContext, PitRoomFn,
};
use mobpit_crawler::{level_floor, KillReceipt, RecordKillInput};
use serde::{Deserialize, Serialize};

pub use game_sim::mobpit::{pit_step, PitInput, PitStep};

/// Entity id of the stats entity (the zone's own HUD view).
pub const PIT_ENTITY: EntityId = EntityId(900);

/// A player: the Arena's predicted [`MoveState`] plus server-owned combat
/// state.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "player", id = 1, priority = 2.0, of = Player)]
pub struct PlayerView {
    /// Position x (room-local; past ±HALF in a doorway passage).
    #[field_type(rooms::POS)]
    #[correct(ease = 150)]
    #[from(state.x)]
    pub x: f32,
    /// Position y.
    #[field_type(rooms::POS)]
    #[correct(ease = 150)]
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
    /// Every pillar of the floor.
    pub fn all() -> Vec<Pillar> {
        PILLARS
            .iter()
            .enumerate()
            .map(|(i, &(x, y, r))| Pillar {
                id: EntityId(100 + i as u32),
                x,
                y,
                r,
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
}

/// Mob Pit's client surface (also used by the leaderboard page).
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<MobPit>("mobpit")
        .events::<PitEvent>()
        .predict::<PitStep>()
        .pure::<PitRoomFn>()
        .constant("pit_rules", &pit_rules())
        .constant("pit_world", &pit_world())
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
const MAX_CREDITS: u8 = 4;
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
const PLAYER_RESPAWN_TICKS: u64 = 90;

/// Mob tier stats: (max hp, speed m/s, damage, radius, telegraph radius).
fn tier_stats(tier: u8) -> (i32, f32, i32, f32, f32) {
    match tier {
        3 => (100, 3.3, 24, 0.8, 2.6),
        2 => (60, 2.4, 16, 0.7, 2.2),
        _ => (30, 3.0, 9, 0.5, 1.8),
    }
}

/// Mob tiers (1..=3).
pub const TIERS: [u8; 3] = [1, 2, 3];
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
}

struct Player {
    id: EntityId,
    principal: String,
    name: String,
    state: MoveState,
    credits: u8,
    hp: i32,
    face: f32,
    swing_cd: u8,
    swing_anim: u8,
    dead_until: Option<u64>,
    level: u32,
    regen: u8,
    /// Handed to a neighbouring room: no more steps here.
    leaving: bool,
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

fn push_out_of_pillars(x: &mut f32, y: &mut f32, radius: f32) {
    for (px, py, pr) in PILLARS {
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
/// neighbour's frame (x y vx vy as f32 LE, dash, cd).
fn encode_carried(s: &MoveState) -> Vec<u8> {
    let mut out = Vec::with_capacity(18);
    for v in [s.x, s.y, s.vx, s.vy] {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out.extend_from_slice(&[s.dash, s.cd]);
    out
}

/// Inverse of [`encode_carried`]; None unless it is a place a player can be.
fn decode_carried(bytes: &[u8]) -> Option<MoveState> {
    if bytes.len() != 18 {
        return None;
    }
    let f = |i: usize| f32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().expect("4 bytes"));
    let state = MoveState {
        x: f(0),
        y: f(1),
        vx: f(2),
        vy: f(3),
        dash: bytes[16],
        cd: bytes[17],
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
        for (id, p) in &mut self.players {
            p.credits = (p.credits + 1).min(MAX_CREDITS);
            p.swing_cd = p.swing_cd.saturating_sub(1);
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
            while p.credits > 0 {
                let Some(input) = ctx.take_inputs(*id, ConsumePolicy::Next).pop() else {
                    break;
                };
                let input = input.input;
                // The declared predicted step: what the client runs as WASM.
                p.state = pit_step(&p.state, &input, &(), movement::DT);
                p.credits -= 1;
                let (sx, sy) = (f32::from(input.mx), f32::from(input.my));
                if sx * sx + sy * sy > 20.0 * 20.0 {
                    p.face = sy.atan2(sx);
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
            if p.regen % 15 == 0 && p.hp < PLAYER_HP {
                p.hp += 1;
            }
            ctx.set_view(*id, View::new(p.state.x, p.state.y, VIEW_RADIUS));
        }
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
            let p = self.players.get_mut(&player).expect("player");
            let carried = encode_carried(&into_neighbour((dx, dy), p.state));
            if parse_room(&target).is_some() && ctx.handoff(player, &target, carried).is_ok() {
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
                m.hp -= ATTACK_DAMAGE;
                if matches!(m.state, MobState::Idle | MobState::Return) {
                    m.state = MobState::Chase;
                    m.target = Some(player);
                }
                if m.hp <= 0 {
                    m.hp = 0;
                    m.state = MobState::Dead;
                    m.respawn_at = ctx.tick() + RESPAWN_TICKS;
                    m.target = None;
                    self.emit_kill(ctx, player, i);
                }
            }
        }
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
        let mut hits: Vec<(PlayerId, i32)> = Vec::new();
        for i in 0..self.mobs.len() {
            let (max_hp, speed, damage, radius, tele) = tier_stats(self.mobs[i].tier);
            let target_pos = |t: Option<PlayerId>| {
                t.and_then(|t| living.iter().find(|(id, _, _)| *id == t))
                    .map(|(_, x, y)| (*x, *y))
            };
            let m = &mut self.mobs[i];
            match m.state {
                MobState::Dead => {
                    if tick >= m.respawn_at {
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
                        .filter(|(_, d)| *d < AGGRO)
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
                            m.face = step_toward(&mut m.x, &mut m.y, tx, ty, speed * dt);
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
                            if dist((m.x, m.y), (*x, *y)) <= tele + PLAYER_RADIUS * 0.5 {
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
                    m.hp = (m.hp + 1).min(max_hp);
                    if dist((m.x, m.y), m.home) < 0.3 {
                        m.state = MobState::Idle;
                        m.wander = m.home;
                    }
                }
            }
            let m = &mut self.mobs[i];
            push_out_of_pillars(&mut m.x, &mut m.y, radius);
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
                hp,
                face,
                swing_cd: 0,
                swing_anim: 0,
                dead_until: None,
                level: 1,
                regen: 0,
                leaving: false,
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

    fn on_create(_ctx: &mut Ctx<'_, Self>, room: RoomConfig) -> Self {
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
                    hp: tier_stats(slot.tier).0,
                    state: MobState::Idle,
                    timer: 0,
                    target: None,
                    generation: 0,
                    respawn_at: 0,
                    wander: home,
                }
            })
            .collect();
        Self {
            room,
            players: BTreeMap::new(),
            mobs,
            pillars: Pillar::all(),
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

    /// A watcher (a player next door, a board page): the whole room.
    fn on_observe(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId) {
        ctx.set_view(observer, View::new(0.0, 0.0, WATCH_RADIUS));
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<PitInput> {
        PitInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        self.tick = ctx.tick();
        self.measure_tick(ctx.now());
        let swings = self.step_players(ctx);
        self.resolve_swings(ctx, swings);
        self.step_mobs(ctx);
        self.walk_through_doorways(ctx);
        self.flush_backlog(ctx);
    }

    fn on_message(&mut self, ctx: &mut Ctx<'_, Self>, message: PitMessage) {
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
                m.hp = saved.hp.clamp(1, tier_stats(m.tier).0);
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
        let max_hp = tier_stats(self.tier).0;
        let tele = if self.state == MobState::Windup {
            255 - (u32::from(self.timer) * 255 / u32::from(WINDUP_TICKS))
        } else {
            0
        };
        Some(MobView::project(
            self,
            MobViewRest {
                hp: (self.hp.max(0) * 100 / max_hp) as u8,
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
}

/// Bot payload: the direction as a move plus attack always held.
pub fn encode_bot_input(dir: [f32; 2]) -> Vec<u8> {
    let axis = |v: f32| (v.clamp(-1.0, 1.0) * 127.0).round() as i8;
    PitInput {
        mx: axis(dir[0]),
        my: axis(dir[1]),
        buttons: PitInput::ATTACK,
    }
    .to_payload()
}
