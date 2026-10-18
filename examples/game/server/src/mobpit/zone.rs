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
//! A kill emits a fact **without awaiting storage**:
//! `Fact::new("kill:{boot}:{zone}:{mob}:{generation}", "crawler.record_kill",
//! {mob, tier, name})` via `ctx.emit_fact_for(killer)` — the host binds the
//! killer's verified principal. `boot` is random per server process, so keys
//! never collide with an earlier run's facts (zone ticks restart at 0). The
//! settlement arrives in [`Zone::on_fact_settled`] on a later tick and is
//! pushed to the killer as [`EVENT_SAVED`] + [`EVENT_PROFILE`].

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use distributed_game::{
    ConsumePolicy, Ctx, EmitError, EntitySchema, EntityState, Fact, FactOutcome, FactSettlement,
    FieldType, FieldValue, KindDef, LeaveReason, ObserverId, PlayerId, View, WorldView, Zone,
};
use serde_json::{json, Value};

use super::crawler::{level_floor, KillReceipt, RECORD_KILL};
use super::world::MobPitWorld;
use crate::arena::movement::{self, MoveInput, MoveState, PILLARS, PLAYER_RADIUS};
use crate::common::{broadcast_json_event, send_json_event, Guests};

/// Player kind.
pub const KIND_PLAYER: u16 = 1;
/// Mob kind.
pub const KIND_MOB: u16 = 2;
/// Pillar kind.
pub const KIND_PILLAR: u16 = 3;
/// Zone stats kind.
pub const KIND_PIT: u16 = 4;
/// Entity id of the stats entity.
pub const PIT_ENTITY: u32 = 900;

/// `{entity, half, boot, attackRange, attackArcCos}` to a joining player.
pub const EVENT_HELLO: u32 = 1;
/// `{xp, level, kills, levelXp, nextLevelXp, source}` — from the cell on
/// join (`source: "cell"`), after each settlement (`"settled"`).
pub const EVENT_PROFILE: u32 = 2;
/// Broadcast `{key, mob, tier, x, y, killer, killerName}` the tick a mob dies.
pub const EVENT_KILL: u32 = 3;
/// To the killer: `{key, ok, replayed, xp, totalXp, kills, level, waitedMs, x, y, error?}`.
pub const EVENT_SAVED: u32 = 4;
/// Broadcast leaderboard snapshot (see `board::Leaderboards::snapshot`).
pub const EVENT_BOARD: u32 = 5;
/// To a player that died: `{respawnMs}`.
pub const EVENT_DIED: u32 = 6;

/// Input bit: attack (bit 0 is the Arena's dash).
pub const BUTTON_ATTACK: u8 = 2;
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

/// Player input: the Arena's 3-byte frame plus [`BUTTON_ATTACK`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PitInput {
    /// Movement part (dash bit only).
    pub movement: MoveInput,
    /// Attack held.
    pub attack: bool,
}

impl MobPit {
    fn profile_event(xp: u64, level: u32, kills: u32, source: &str) -> Value {
        json!({
            "xp": xp,
            "level": level,
            "kills": kills,
            "levelXp": level_floor(level),
            "nextLevelXp": level_floor(level + 1),
            "source": source,
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
        let zone = ctx.zone_id().to_string();
        let key = format!(
            "kill:{}:{zone}:m{}:{}",
            self.world.boot(),
            m.id,
            m.generation
        );
        let (x, y, tier, id) = (m.x, m.y, m.tier, m.id);
        let Some(player) = self.players.get(&killer) else {
            return;
        };
        let fact = Fact::new(
            key.clone(),
            RECORD_KILL,
            json!({ "mob": format!("{zone}/m{id}"), "tier": tier, "name": player.name }),
        );
        broadcast_json_event(
            ctx,
            EVENT_KILL,
            &json!({
                "key": key, "mob": id, "tier": tier, "x": x, "y": y,
                "killer": player.entity, "killerName": player.name,
            }),
        );
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
                p.state = movement::step(p.state, input.movement);
                p.credits -= 1;
                let (sx, sy) = (f32::from(input.movement.mx), f32::from(input.movement.my));
                if sx * sx + sy * sy > 20.0 * 20.0 {
                    p.face = sy.atan2(sx);
                }
                if input.attack && p.swing_cd == 0 {
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
                send_json_event(
                    ctx,
                    id,
                    EVENT_DIED,
                    &json!({ "respawnMs": PLAYER_RESPAWN_TICKS * 1000 / 30 }),
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
            send_json_event(
                ctx,
                id,
                EVENT_PROFILE,
                &Self::profile_event(xp, level, kills, "cell"),
            );
        }
    }

    fn push_board(&mut self, ctx: &mut Ctx<'_, Self>) {
        let version = self.world.board().version();
        if version != self.board_version {
            self.board_version = version;
            let snapshot = self.world.board().snapshot();
            broadcast_json_event(ctx, EVENT_BOARD, &snapshot);
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
        let pos = FieldType::QuantF32 {
            min: -movement::HALF,
            max: movement::HALF,
            bits: 14,
        };
        EntitySchema::new()
            .kind(
                KindDef::new(KIND_PLAYER, "player")
                    .field("x", movement::POS)
                    .field("y", movement::POS)
                    .field("vx", movement::VEL)
                    .field("vy", movement::VEL)
                    .field("dash", FieldType::U8)
                    .field("cd", FieldType::U8)
                    .field_with_priority("name", FieldType::ShortStr, 0.2)
                    .field("hp", FieldType::U8)
                    .field("face", FieldType::Angle { bits: 8 })
                    .field("swing", FieldType::U8)
                    .field("level", FieldType::U16)
                    .field("dead", FieldType::Bool)
                    .priority(2.0),
            )
            .kind(
                KindDef::new(KIND_MOB, "mob")
                    .field("x", pos)
                    .field("y", pos)
                    .field("face", FieldType::Angle { bits: 8 })
                    .field("tier", FieldType::U8)
                    .field("hp", FieldType::U8)
                    .field("state", FieldType::U8)
                    .field("tele", FieldType::U8),
            )
            .kind(
                KindDef::new(KIND_PILLAR, "pillar")
                    .field("x", FieldType::F32)
                    .field("y", FieldType::F32)
                    .field("r", FieldType::F32),
            )
            .kind(
                KindDef::new(KIND_PIT, "pit")
                    .field("pending", FieldType::U16)
                    .field("slowMs", FieldType::U16)
                    .field("tickMs", FieldType::F32)
                    .field("tickMaxMs", FieldType::F32)
                    .field("workUs", FieldType::U32)
                    .field("committed", FieldType::U32)
                    .field("backlog", FieldType::U16),
            )
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
        send_json_event(
            ctx,
            player,
            EVENT_HELLO,
            &json!({
                "entity": entity, "half": movement::HALF, "boot": self.world.boot(),
                "attackRange": ATTACK_RANGE, "attackArcCos": ATTACK_ARC_COS,
            }),
        );
        send_json_event(ctx, player, EVENT_BOARD, &self.world.board().snapshot());
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
        send_json_event(ctx, observer, EVENT_BOARD, &self.world.board().snapshot());
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<PitInput> {
        let movement = MoveInput::decode(payload)?;
        Some(PitInput {
            movement,
            attack: payload[2] & BUTTON_ATTACK != 0,
        })
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
                p.emitted.elapsed().as_millis() as u64,
            ),
            None => (String::new(), 0.0, 0.0, 0),
        };
        let player = self.player_by_principal(&principal);
        match settlement.outcome {
            FactOutcome::Committed(receipt) => {
                self.committed += 1;
                let Ok(kill) = serde_json::from_value::<KillReceipt>(receipt.payload) else {
                    return;
                };
                let Some(player) = player else { return };
                if let Some(p) = self.players.get_mut(&player) {
                    p.level = kill.level;
                }
                send_json_event(
                    ctx,
                    player,
                    EVENT_SAVED,
                    &json!({
                        "key": settlement.key, "ok": true, "replayed": receipt.replayed,
                        "xp": kill.xp, "totalXp": kill.total_xp, "kills": kill.kills,
                        "level": kill.level, "waitedMs": waited_ms, "x": x, "y": y,
                    }),
                );
                send_json_event(
                    ctx,
                    player,
                    EVENT_PROFILE,
                    &Self::profile_event(kill.total_xp, kill.level, kill.kills, "settled"),
                );
            }
            FactOutcome::Failed { error, attempts } => {
                if let Some(player) = player {
                    send_json_event(
                        ctx,
                        player,
                        EVENT_SAVED,
                        &json!({
                            "key": settlement.key, "ok": false, "error": format!("{error:?}"),
                            "attempts": attempts, "waitedMs": waited_ms, "x": x, "y": y,
                        }),
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
            out.push(EntityState::new(
                p.entity,
                KIND_PLAYER,
                s.x,
                s.y,
                vec![
                    FieldValue::F(s.x),
                    FieldValue::F(s.y),
                    FieldValue::F(s.vx),
                    FieldValue::F(s.vy),
                    FieldValue::U(s.dash.into()),
                    FieldValue::U(s.cooldown.into()),
                    FieldValue::Str(p.name.clone()),
                    FieldValue::U(p.hp.max(0) as u32),
                    FieldValue::F(p.face),
                    FieldValue::U(p.swing_anim.into()),
                    FieldValue::U(p.level),
                    FieldValue::Bool(p.dead_until.is_some()),
                ],
            ));
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
            out.push(EntityState::new(
                m.id,
                KIND_MOB,
                m.x,
                m.y,
                vec![
                    FieldValue::F(m.x),
                    FieldValue::F(m.y),
                    FieldValue::F(m.face),
                    FieldValue::U(m.tier.into()),
                    FieldValue::U((m.hp.max(0) * 100 / max_hp) as u32),
                    FieldValue::U(m.state as u32),
                    FieldValue::U(tele),
                ],
            ));
        }
        for (i, (x, y, r)) in PILLARS.iter().enumerate() {
            out.push(
                EntityState::new(
                    100 + i as u32,
                    KIND_PILLAR,
                    *x,
                    *y,
                    vec![FieldValue::F(*x), FieldValue::F(*y), FieldValue::F(*r)],
                )
                .global(),
            );
        }
        out.push(
            EntityState::new(
                PIT_ENTITY,
                KIND_PIT,
                0.0,
                0.0,
                vec![
                    FieldValue::U(self.pending.len().min(65_535) as u32),
                    FieldValue::U(self.world.sink_state().latency().as_millis().min(65_535) as u32),
                    FieldValue::F(self.tick_ms),
                    FieldValue::F(self.tick_max_ms),
                    FieldValue::U(self.work_us as u32),
                    FieldValue::U(self.committed),
                    FieldValue::U(self.backlog.len().min(65_535) as u32),
                ],
            )
            .global(),
        );
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
    vec![axis(dir[0]) as u8, axis(dir[1]) as u8, BUTTON_ATTACK]
}
