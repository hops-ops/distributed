//! Mob Pit, trimmed for the spike: the same players, mobs, pillars, melee
//! combat and the same replicated kinds/ids as
//! `examples/game/server/src/mobpit/zone.rs` (copied, not shared: that zone
//! lives in the native `game_demos` crate next to tokio, reqwest and the
//! Crawler fact sink, so it does not build for wasm32 as is).
//!
//! Left out: kill facts / Crawler XP settlement, leaderboard, profiles.
//! Added: the global `host` kind (which celld node, isolate and activation
//! run the room, tick numbers) and a hibernation snapshot (mobs, kill
//! counts) that the Durable Object keeps in its storage.

use std::collections::BTreeMap;

use distributed_game::{
    schema, ConsumePolicy, Ctx, Entities, Entity, EntityId, EntitySchema, GameEvent, GameInput,
    LeaveReason, ObserverId, PlayerId, Project, Replicated, View, Zone,
};
use game_sim::arena::{self as movement, MoveState, PILLARS, PLAYER_RADIUS};
pub use game_sim::mobpit::{pit_step, PitInput};
use serde::{Deserialize, Serialize};

/// Entity id of the zone's HUD view (as in Mob Pit).
pub const PIT_ENTITY: EntityId = EntityId(900);
/// Entity id of the host view (spike only).
pub const HOST_ENTITY: EntityId = EntityId(901);

/// A player (Mob Pit's `player` kind, id 1).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "player", id = 1, priority = 2.0, of = Player)]
pub struct PlayerView {
    #[field_type(movement::POS)]
    #[correct(ease = 150)]
    #[from(state.x)]
    pub x: f32,
    #[field_type(movement::POS)]
    #[correct(ease = 150)]
    #[from(state.y)]
    pub y: f32,
    #[field_type(movement::VEL)]
    #[correct(snap)]
    #[from(state.vx)]
    pub vx: f32,
    #[field_type(movement::VEL)]
    #[correct(snap)]
    #[from(state.vy)]
    pub vy: f32,
    #[correct(snap)]
    #[from(state.dash)]
    pub dash: u8,
    #[correct(snap)]
    #[from(state.cd)]
    pub cd: u8,
    #[priority(0.2)]
    #[correct(replace)]
    #[from]
    pub name: String,
    #[correct(replace)]
    pub hp: u8,
    #[angle(bits = 8)]
    #[correct(replace)]
    #[from]
    pub face: f32,
    #[correct(replace)]
    pub swing: u8,
    /// Kills by this player in this room (persisted with the room).
    #[correct(replace)]
    pub level: u16,
    #[correct(replace)]
    pub dead: bool,
}

/// A mob (Mob Pit's `mob` kind, id 2).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "mob", id = 2, of = Mob)]
pub struct MobView {
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    #[from]
    pub x: f32,
    #[quant(min = -movement::HALF, max = movement::HALF, bits = 14)]
    #[from]
    pub y: f32,
    #[angle(bits = 8)]
    #[from]
    pub face: f32,
    #[from]
    pub tier: u8,
    pub hp: u8,
    pub state: u8,
    pub tele: u8,
}

/// A pillar (the Arena's `pillar` kind, id 3).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "pillar", id = 3, global, of = Pillar)]
pub struct PillarView {
    #[from]
    pub x: f32,
    #[from]
    pub y: f32,
    #[from]
    pub r: f32,
}

/// Room numbers (global).
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "pit", id = 4, global, of = Pit)]
pub struct PitView {
    /// Kills in this room since it was first created (persisted).
    pub kills: u32,
    /// Ticks this room has simulated across all activations (persisted).
    pub ticks: u32,
}

/// Where the room runs (global, spike only): set by the Durable Object.
#[derive(Debug, Clone, PartialEq, Replicated)]
#[replicated(kind = "host", id = 5, global, of = HostInfo)]
pub struct HostView {
    /// celld node id (`CELLD_NODE`), found through the operator listeners.
    #[from]
    pub node: String,
    /// Random id of the V8 isolate (module instance) running the room.
    #[from]
    pub isolate: String,
    /// Random id of this activation of the object (new on every wake).
    #[from]
    pub activation: String,
    /// Activations of this room so far (persisted).
    #[from]
    pub activations: u32,
    /// The room was restored from storage on this activation.
    #[from]
    pub restored: bool,
    /// Cold start: object constructor to first tick (ms).
    #[from]
    pub wake_ms: u32,
    /// Tick rate (Hz).
    #[from]
    pub hz: u8,
    /// Loop wake lateness EMA (ms; how late the timer fired).
    #[from]
    pub late_ms: f32,
    /// Max lateness over the last second (ms).
    #[from]
    pub late_max_ms: f32,
    /// Work per loop iteration EMA (µs: advance + sends).
    #[from]
    pub work_us: u32,
    /// Rooms resident in the same isolate.
    #[from]
    pub isolate_rooms: u16,
}

/// A static pillar (the Arena's floor).
pub struct Pillar {
    id: EntityId,
    pub x: f32,
    pub y: f32,
    pub r: f32,
}

impl Pillar {
    fn all() -> Vec<Pillar> {
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

/// The host info the Durable Object keeps current.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HostInfo {
    pub node: String,
    pub isolate: String,
    pub activation: String,
    pub activations: u32,
    pub restored: bool,
    pub wake_ms: u32,
    pub hz: u8,
    pub late_ms: f32,
    pub late_max_ms: f32,
    pub work_us: u32,
    pub isolate_rooms: u16,
}

impl Entity for HostInfo {
    fn entity_id(&self) -> EntityId {
        HOST_ENTITY
    }
}

/// Mob Pit's events (ids as in Mob Pit; `Kill` trimmed).
#[derive(Debug, Clone, PartialEq, GameEvent)]
pub enum PitEvent {
    #[game_event(id = 1)]
    Hello {
        entity: u32,
        half: f32,
        boot: String,
        attack_range: f32,
        attack_arc_cos: f32,
    },
    #[game_event(id = 3)]
    Kill {
        mob: u32,
        tier: u8,
        killer: u32,
        killer_name: String,
        /// The killer's kills in this room.
        kills: u32,
    },
    #[game_event(id = 6)]
    Died { respawn_ms: u32 },
}

pub const ATTACK_RANGE: f32 = 2.2;
pub const ATTACK_ARC_COS: f32 = 0.5;
pub const ATTACK_DAMAGE: i32 = 10;
pub const SWING_COOLDOWN: u8 = 13;
const SWING_FRAMES: u8 = 6;
pub const PLAYER_HP: i32 = 100;
pub const MOBS: usize = 12;
const MAX_CREDITS: u8 = 4;
const VIEW_RADIUS: f32 = 40.0;
const AGGRO: f32 = 7.0;
const LEASH: f32 = 15.0;
const WINDUP_TICKS: u16 = 24;
const RECOVER_TICKS: u16 = 18;
const RESPAWN_TICKS: u64 = 120;
const PLAYER_RESPAWN_TICKS: u64 = 90;

fn tier_stats(tier: u8) -> (i32, f32, i32, f32, f32) {
    match tier {
        3 => (100, 3.3, 24, 0.8, 2.6),
        2 => (60, 2.4, 16, 0.7, 2.2),
        _ => (30, 3.0, 9, 0.5, 1.8),
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

#[derive(Serialize, Deserialize)]
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
    #[serde(skip)]
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
    regen: u8,
    kills: u32,
}

/// The persisted part of a room.
#[derive(Serialize, Deserialize)]
struct Snapshot {
    mobs: Vec<Mob>,
    kills: u32,
    ticks: u64,
    activations: u32,
    kills_by: BTreeMap<String, u32>,
}

/// One Mob Pit room.
pub struct Pit {
    players: BTreeMap<PlayerId, Player>,
    mobs: Vec<Mob>,
    pillars: Vec<Pillar>,
    /// Lifetime kills (persisted).
    pub kills: u32,
    /// Lifetime ticks (persisted; this activation adds `ctx.tick()`).
    pub ticks_before: u64,
    tick: u64,
    /// Kills per principal (persisted).
    pub kills_by: BTreeMap<String, u32>,
    /// Activations including this one (persisted).
    pub activations: u32,
    /// Host info (set by the Durable Object).
    pub host: HostInfo,
}

fn mob_layout() -> Vec<(u8, (f32, f32))> {
    (0..MOBS)
        .map(|i| {
            let ring = if i % 2 == 0 { 11.5 } else { 16.0 };
            let a = (i as f32 + 0.5) / MOBS as f32 * std::f32::consts::TAU;
            let tier = match i {
                0 => 3,
                3 | 6 | 9 | 11 => 2,
                _ => 1,
            };
            (tier, (ring * a.cos(), ring * a.sin()))
        })
        .collect()
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

impl Pit {
    /// Lifetime ticks.
    pub fn ticks(&self) -> u64 {
        self.ticks_before + self.tick
    }

    /// Kills of one principal in this room.
    pub fn kills_of(&self, principal: &str) -> u32 {
        self.kills_by.get(principal).copied().unwrap_or(0)
    }

    fn step_players(&mut self, ctx: &mut Ctx<'_, Self>) -> Vec<(PlayerId, f32, f32, f32)> {
        let tick = ctx.tick();
        let mut swings = Vec::new();
        for (id, p) in &mut self.players {
            p.credits = (p.credits + 1).min(MAX_CREDITS);
            p.swing_cd = p.swing_cd.saturating_sub(1);
            p.swing_anim = p.swing_anim.saturating_sub(1);
            if let Some(until) = p.dead_until {
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
                    let (mob, tier) = (m.id, m.tier);
                    self.kills += 1;
                    if let Some(p) = self.players.get_mut(&player) {
                        let kills = self.kills_by.entry(p.principal.clone()).or_default();
                        *kills += 1;
                        let kills = *kills;
                        p.kills = kills;
                        ctx.broadcast(&PitEvent::Kill {
                            mob,
                            tier,
                            killer: p.id.get(),
                            killer_name: p.name.clone(),
                            kills,
                        });
                    }
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
}

impl Zone for Pit {
    type Config = ();
    type Input = PitInput;
    type Timer = ();
    type Message = distributed_game::NoMessages;

    fn schema() -> EntitySchema {
        schema![PlayerView, MobView, PillarView, PitView, HostView]
    }

    fn entities() -> Entities<Self> {
        Entities::new()
            .each(|z: &Self| &z.players)
            .each(|z: &Self| &z.mobs)
            .each(|z: &Self| &z.pillars)
            .one(|z: &Self| z)
            .one(|z: &Self| &z.host)
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _config: ()) -> Self {
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
            players: BTreeMap::new(),
            mobs,
            pillars: Pillar::all(),
            kills: 0,
            ticks_before: 0,
            tick: 0,
            kills_by: BTreeMap::new(),
            activations: 1,
            host: HostInfo::default(),
        }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let (principal, name) = ctx
            .player(player)
            .map(|p| (p.principal.clone(), p.name.clone()))
            .unwrap_or_default();
        let name = name.unwrap_or_else(|| principal.clone());
        let kills = self.kills_of(&principal);
        let (x, y) = spawn_point(player);
        let entity = EntityId(1000 + player.0);
        self.players.insert(
            player,
            Player {
                id: entity,
                principal,
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
                regen: 0,
                kills,
            },
        );
        ctx.set_view(player, View::new(x, y, VIEW_RADIUS));
        ctx.send(
            player,
            &PitEvent::Hello {
                entity: entity.get(),
                half: movement::HALF,
                boot: self.host.activation.clone(),
                attack_range: ATTACK_RANGE,
                attack_arc_cos: ATTACK_ARC_COS,
            },
        );
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
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<PitInput> {
        PitInput::from_payload(payload)
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        self.tick = ctx.tick();
        let swings = self.step_players(ctx);
        self.resolve_swings(ctx, swings);
        self.step_mobs(ctx);
    }

    fn hibernate(&self) -> Option<Vec<u8>> {
        #[derive(Serialize)]
        struct SnapshotRef<'a> {
            mobs: &'a [Mob],
            kills: u32,
            ticks: u64,
            activations: u32,
            kills_by: &'a BTreeMap<String, u32>,
        }
        serde_json::to_vec(&SnapshotRef {
            mobs: &self.mobs,
            kills: self.kills,
            ticks: self.ticks(),
            activations: self.activations,
            kills_by: &self.kills_by,
        })
        .ok()
    }

    fn on_restore(&mut self, _ctx: &mut Ctx<'_, Self>, state: &[u8], _saved_version: &str) {
        let Ok(snapshot) = serde_json::from_slice::<Snapshot>(state) else {
            return;
        };
        // Respawn timers are tick numbers of the old activation.
        self.mobs = snapshot
            .mobs
            .into_iter()
            .map(|mut m| {
                m.respawn_at = m
                    .respawn_at
                    .saturating_sub(snapshot.ticks.min(m.respawn_at));
                if m.state != MobState::Dead {
                    m.state = MobState::Return;
                }
                m
            })
            .collect();
        self.kills = snapshot.kills;
        self.ticks_before = snapshot.ticks;
        self.activations = snapshot.activations + 1;
        self.kills_by = snapshot.kills_by;
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
                level: self.kills.min(u32::from(u16::MAX)) as u16,
                dead: self.dead_until.is_some(),
            },
        ))
    }
}

impl Entity for Mob {
    fn entity_id(&self) -> EntityId {
        EntityId(self.id)
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

impl Entity for Pit {
    fn entity_id(&self) -> EntityId {
        PIT_ENTITY
    }
}

impl Project<PitView> for Pit {
    fn view(&self) -> Option<PitView> {
        Some(PitView::project(
            self,
            PitViewRest {
                kills: self.kills,
                ticks: self.ticks().min(u64::from(u32::MAX)) as u32,
            },
        ))
    }
}
