//! The Mob Pit zone: authoritative melee combat at 30 Hz.
//!
//! Movement reuses the Arena's predicted step ([`crate::arena::movement`],
//! same floor and pillars) so the own capsule is client-predicted; combat is
//! server-only. Entities:
//!
//! | kind | ids | fields |
//! |---|---|---|
//! | 1 `player` | 1000 + player | x y vx vy dash cd (predicted) · name hp face swing level dead |
//! | 2 `mob`    | 1..=12 | x y face tier hp state tele |
//! | 3 `pillar` | 100 + i | x y r (global) |
//! | 4 `pit`    | 900 | pending slowMs tickMs tickMaxMs workUs committed backlog (global) |
//!
//! Mob state machine (deterministic, zone RNG only for wander targets):
//! `idle` (wander near home) → `chase` (living player within aggro radius or
//! hit by one) → `windup` (in reach: stand still, telegraph ring for 0.8 s) →
//! damage everyone inside the ring → `recover` (0.6 s) → `chase`; beyond the
//! leash or target dead → `return` home → `idle`. Dead mobs respawn after 4 s
//! with a new generation.
//!
//! Kinds are `#[derive(Replicated)]` views, events the typed [`PitEvent`],
//! input the `#[derive(GameInput)]` [`PitInput`]; the browser client is
//! generated from [`game_surface`].
//!
//! A kill emits a typed fact **without awaiting storage**: the cell
//! command's own input, [`RecordKillInput`] `{mob, tier, name}`, keyed by
//! `ctx.fact_with_key(&input, &[m{mob}, generation])` and emitted with
//! `ctx.emit_fact_for(killer)` — the host binds the killer's verified
//! principal and the handler decides the XP. The key is
//! `crawler.record_kill:{zone}:{boot_epoch}:m{mob}:{generation}`: the boot
//! epoch grows across restarts, so keys never collide with an earlier run's
//! facts (generations and ticks restart at 0). The settlement arrives in
//! [`Zone::on_fact_settled`] on a later tick and is pushed to the killer as
//! [`PitEvent::Saved`] + [`PitEvent::Profile`].

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use distributed_game::{
    schema, ConsumePolicy, Ctx, EmitError, EntitySchema, Fact, FactOutcome, FactSettlement,
    GameEvent, GameInput, GameRecord, GameSurface, LeaveReason, ObserverId, PlayerId, Replicated,
    View, WorldView, Zone,
};

use super::board::BoardSnapshot;
use super::crawler::{level_floor, KillReceipt, RecordKillInput};
use super::world::MobPitWorld;
use crate::arena::movement::{self, MoveInput, MoveState, PILLARS, PLAYER_RADIUS};
use crate::arena::PillarView;
use crate::common::Guests;

/// Entity id of the stats entity.
pub const PIT_ENTITY: u32 = 900;

/// A player: the Arena's predicted [`MoveState`] plus server-owned combat
/// state (replaced, never predicted, on the client).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "player", id = 1, priority = 2.0)]
pub struct PlayerView {
    /// Position x.
    #[field_type(movement::POS)]
    #[correct(ease = 150)]
    pub x: f32,
    /// Position y.
    #[field_type(movement::POS)]
    #[correct(ease = 150)]
    pub y: f32,
    /// Velocity x.
    #[field_type(movement::VEL)]
    #[correct(snap)]
    pub vx: f32,
    /// Velocity y.
    #[field_type(movement::VEL)]
    #[correct(snap)]
    pub vy: f32,
    /// Remaining dash frames.
    #[correct(snap)]
    pub dash: u8,
    /// Remaining dash cooldown frames.
    #[correct(snap)]
    pub cd: u8,
    /// Display name.
    #[priority(0.2)]
    #[correct(replace)]
    pub name: String,
    /// Hit points.
    #[correct(replace)]
    pub hp: u8,
    /// Facing (rad).
    #[angle(bits = 8)]
    #[correct(replace)]
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
#[replicated(kind = "mob", id = 2)]
pub struct MobView {
    /// Position x.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub x: f32,
    /// Position y.
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    pub y: f32,
    /// Facing (rad).
    #[angle(bits = 8)]
    pub face: f32,
    /// 1 grunt · 2 brute · 3 elite.
    pub tier: u8,
    /// HP percent.
    pub hp: u8,
    /// 0 idle · 1 chase · 2 windup · 3 recover · 4 return.
    pub state: u8,
    /// Telegraph progress 0..=255 during a windup.
    pub tele: u8,
}

/// Zone numbers for the HUD (global).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "pit", id = 4, global)]
pub struct PitView {
    /// Facts emitted and not settled.
    pub pending: u16,
    /// Artificial storage latency (ms).
    pub slow_ms: u16,
    /// Tick interval EMA (ms).
    pub tick_ms: f32,
    /// Longest tick interval of the last 30 ticks (ms).
    pub tick_max_ms: f32,
    /// Tick work EMA (µs).
    pub work_us: u32,
    /// Settlements received.
    pub committed: u32,
    /// Facts waiting for backpressure to clear.
    pub backlog: u16,
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
        /// Process boot id (changes on restart).
        boot: String,
        /// Melee reach (m).
        attack_range: f32,
        /// Cosine of the half cone angle.
        attack_arc_cos: f32,
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
    /// Leaderboard snapshot (on join and whenever it changes).
    #[game_event(id = 5)]
    Board(BoardSnapshot),
    /// To a player that died.
    #[game_event(id = 6)]
    Died {
        /// Respawn delay (ms).
        respawn_ms: u32,
    },
}

/// Mob Pit's client surface (also used by the leaderboard page).
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<MobPit>("mobpit").events::<PitEvent>()
}

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
/// Number of mobs.
pub const MOBS: usize = 12;
const MAX_CREDITS: u8 = 4;
const VIEW_RADIUS: f32 = 40.0;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MobState {
    Idle = 0,
    Chase = 1,
    Windup = 2,
    Recover = 3,
    Return = 4,
    Dead = 5,
}

struct Mob {
    id: u32,
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
    entity: u32,
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
}

struct PendingKill {
    principal: String,
    x: f32,
    y: f32,
    emitted: Instant,
}

/// Zone construction parameters.
#[derive(Clone)]
pub struct MobPitConfig {
    /// Display names from the dev ticket endpoint.
    pub guests: Guests,
    /// Durable cells, fact sink state and leaderboards.
    pub world: Arc<MobPitWorld>,
}

/// The Mob Pit zone.
pub struct MobPit {
    guests: Guests,
    world: Arc<MobPitWorld>,
    players: BTreeMap<PlayerId, Player>,
    mobs: Vec<Mob>,
    pending: HashMap<String, PendingKill>,
    /// Facts refused by backpressure, retried each tick (oldest first).
    backlog: VecDeque<(PlayerId, Fact, f32, f32)>,
    board_version: u64,
    last_tick: Option<Instant>,
    tick_ms: f32,
    tick_max_ms: f32,
    tick_max_window: f32,
    window_ticks: u32,
    work_us: f32,
    committed: u32,
}

/// Mob homes on two rings between the pillars, tiers by index.
fn mob_layout() -> Vec<(u8, (f32, f32))> {
    let mut out = Vec::with_capacity(MOBS);
    for i in 0..MOBS {
        let ring = if i % 2 == 0 { 11.5 } else { 16.0 };
        let a = (i as f32 + 0.5) / MOBS as f32 * std::f32::consts::TAU;
        let tier = match i {
            0 => 3,
            3 | 6 | 9 | 11 => 2,
            _ => 1,
        };
        out.push((tier, (ring * a.cos(), ring * a.sin())));
    }
    out
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

impl MobPit {
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

    fn emit_kill(&mut self, ctx: &mut Ctx<'_, Self>, killer: PlayerId, mob: usize) {
        let m = &self.mobs[mob];
        let (x, y, tier, id, generation) = (m.x, m.y, m.tier, m.id, m.generation);
        let Some(player) = self.players.get(&killer) else {
            return;
        };
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
            killer: player.entity,
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
                            emitted: Instant::now(),
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
            while p.credits > 0 {
                let Some(input) = ctx.take_inputs(*id, ConsumePolicy::Next).pop() else {
                    break;
                };
                let input = input.input;
                p.state = movement::step(p.state, input.movement());
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
            }
            p.regen = p.regen.wrapping_add(1);
            if p.regen % 15 == 0 && p.hp < PLAYER_HP {
                p.hp += 1;
            }
            ctx.set_view(*id, View::new(p.state.x, p.state.y, VIEW_RADIUS));
        }
        swings
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

    fn deliver_profiles(&mut self, ctx: &mut Ctx<'_, Self>) {
        for (principal, crawler) in self.world.take_loaded_profiles() {
            let Some(id) = self.player_by_principal(&principal) else {
                continue;
            };
            let (xp, level, kills) = crawler.unwrap_or((0, 1, 0));
            if let Some(p) = self.players.get_mut(&id) {
                p.level = level;
            }
            ctx.send(id, &Self::profile_event(xp, level, kills, "cell"));
        }
    }

    fn push_board(&mut self, ctx: &mut Ctx<'_, Self>) {
        let version = self.world.board().version();
        if version != self.board_version {
            self.board_version = version;
            let snapshot = self.world.board().snapshot();
            ctx.broadcast(&PitEvent::Board((*snapshot).clone()));
        }
    }

    fn measure_tick(&mut self) {
        let now = Instant::now();
        if let Some(last) = self.last_tick {
            let ms = now.duration_since(last).as_secs_f32() * 1000.0;
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
}

impl Zone for MobPit {
    type Config = MobPitConfig;
    type Input = PitInput;
    type Timer = ();

    fn schema() -> EntitySchema {
        schema![PlayerView, MobView, PillarView, PitView]
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, config: MobPitConfig) -> Self {
        let mobs = mob_layout()
            .into_iter()
            .enumerate()
            .map(|(i, (tier, home))| Mob {
                id: i as u32 + 1,
                tier,
                home,
                x: home.0,
                y: home.1,
                face: 0.0,
                hp: tier_stats(tier).0,
                state: MobState::Idle,
                timer: 0,
                target: None,
                generation: 0,
                respawn_at: 0,
                wander: home,
            })
            .collect();
        Self {
            guests: config.guests,
            world: config.world,
            players: BTreeMap::new(),
            mobs,
            pending: HashMap::new(),
            backlog: VecDeque::new(),
            board_version: u64::MAX,
            last_tick: None,
            tick_ms: 0.0,
            tick_max_ms: 0.0,
            tick_max_window: 0.0,
            window_ticks: 0,
            work_us: 0.0,
            committed: 0,
        }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let principal = ctx
            .player(player)
            .map(|p| p.principal.clone())
            .unwrap_or_default();
        let name = self
            .guests
            .name(&principal)
            .unwrap_or_else(|| principal.clone());
        let (x, y) = spawn_point(player);
        let entity = 1000 + player.0;
        self.players.insert(
            player,
            Player {
                entity,
                principal: principal.clone(),
                name,
                state: MoveState {
                    x,
                    y,
                    ..MoveState::default()
                },
                credits: MAX_CREDITS,
                hp: PLAYER_HP,
                face: 0.0,
                swing_cd: 0,
                swing_anim: 0,
                dead_until: None,
                level: 1,
                regen: 0,
            },
        );
        ctx.set_view(player, View::new(x, y, VIEW_RADIUS));
        ctx.send(
            player,
            &PitEvent::Hello {
                entity,
                half: movement::HALF,
                boot: self.world.boot().to_string(),
                attack_range: ATTACK_RANGE,
                attack_arc_cos: ATTACK_ARC_COS,
            },
        );
        ctx.send(
            player,
            &PitEvent::Board((*self.world.board().snapshot()).clone()),
        );
        // Read path: load the durable cell off the tick; delivered later.
        self.world.request_profile(&principal);
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, _reason: LeaveReason) {
        self.players.remove(&player);
        for m in &mut self.mobs {
            if m.target == Some(player) {
                m.target = None;
            }
        }
    }

    fn on_observe(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId) {
        ctx.set_view(observer, View::new(0.0, 0.0, VIEW_RADIUS));
        ctx.send(
            observer,
            &PitEvent::Board((*self.world.board().snapshot()).clone()),
        );
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<PitInput> {
        PitInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        self.measure_tick();
        let work = Instant::now();
        self.deliver_profiles(ctx);
        let swings = self.step_players(ctx);
        self.resolve_swings(ctx, swings);
        self.step_mobs(ctx);
        self.flush_backlog(ctx);
        self.push_board(ctx);
        self.world
            .set_zone_pending(ctx.facts_in_flight() + self.backlog.len());
        let us = work.elapsed().as_secs_f32() * 1e6;
        self.work_us = if self.work_us == 0.0 {
            us
        } else {
            self.work_us * 0.9 + us * 0.1
        };
    }

    fn on_fact_settled(&mut self, ctx: &mut Ctx<'_, Self>, settlement: FactSettlement) {
        let pending = self.pending.remove(&settlement.key);
        let (principal, x, y, waited_ms) = match &pending {
            Some(p) => (
                p.principal.clone(),
                p.x,
                p.y,
                p.emitted.elapsed().as_millis().min(u128::from(u32::MAX)) as u32,
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

    fn replicate(&self, out: &mut WorldView) {
        for p in self.players.values() {
            let s = p.state;
            let view = PlayerView {
                x: s.x,
                y: s.y,
                vx: s.vx,
                vy: s.vy,
                dash: s.dash,
                cd: s.cooldown,
                name: p.name.clone(),
                hp: p.hp.clamp(0, 255) as u8,
                face: p.face,
                swing: p.swing_anim,
                level: p.level.min(u32::from(u16::MAX)) as u16,
                dead: p.dead_until.is_some(),
            };
            out.add(p.entity, &view);
        }
        for m in &self.mobs {
            if m.state == MobState::Dead {
                continue;
            }
            let max_hp = tier_stats(m.tier).0;
            let tele = if m.state == MobState::Windup {
                255 - (u32::from(m.timer) * 255 / u32::from(WINDUP_TICKS))
            } else {
                0
            };
            let view = MobView {
                x: m.x,
                y: m.y,
                face: m.face,
                tier: m.tier,
                hp: (m.hp.max(0) * 100 / max_hp) as u8,
                state: m.state as u8,
                tele: tele as u8,
            };
            out.add(m.id, &view);
        }
        for (i, (x, y, r)) in PILLARS.iter().enumerate() {
            out.add(
                100 + i as u32,
                &PillarView {
                    x: *x,
                    y: *y,
                    r: *r,
                },
            );
        }
        let clamp16 = |n: usize| n.min(65_535) as u16;
        let stats = PitView {
            pending: clamp16(self.pending.len()),
            slow_ms: self.world.sink_state().latency().as_millis().min(65_535) as u16,
            tick_ms: self.tick_ms,
            tick_max_ms: self.tick_max_ms,
            work_us: self.work_us as u32,
            committed: self.committed,
            backlog: clamp16(self.backlog.len()),
        };
        out.add(PIT_ENTITY, &stats);
    }
}

/// Zone numbers for tests and tooling (`GameServer::inspect`).
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
        }
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
