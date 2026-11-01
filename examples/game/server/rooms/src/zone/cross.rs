//! Play across room boundaries, over zone messages
//! ([`distributed_game::zone_message`]): a laser shot carries on into the
//! room next door, and a mob chasing a player who leaves through a side
//! follows it across.
//!
//! **Laser legs.** A beam that leaves the room through a side (an open
//! side, a doorway, over a wall) hands the rest of its path to the room
//! beyond ([`PitMessage::LaserLeg`], sent on behalf of the shooter). That
//! room resolves it against its own geometry and its own mob history, as
//! old as the shooter's view was (the age, not a clock time: rooms do not
//! share clocks), and credits a kill to the shooter's verified principal.
//! Everyone watching it sees the beam ([`PitEvent::Beam`]).
//!
//! **Mob crossings: a lease.** The home room keeps the mob's slot *away*
//! (hidden, [`Away`]); the room it crossed into holds it as a *visitor*
//! ([`Visit`]) while its lease holds. The visitor beats every second
//! ([`PitMessage::MobBeat`]); the home room answers with a renewal
//! ([`PitMessage::MobLease`]) or a recall. A visitor whose lease lapses
//! ([`VISIT_LEASE`], 2 s since the last renewal) is dropped; an away slot
//! with no beat for [`AWAY_GRACE`] (4 s) comes back home. The visitor
//! always lapses first, so the mob is never live in two rooms; a lost
//! message or a crashed or sleeping room costs it at most the grace, never
//! the mob. A visitor that loses its target walks back to where it came in
//! and goes home ([`PitMessage::MobHome`]); one that is killed tells its
//! home ([`PitMessage::MobFell`]). Its kill fact is keyed by its home
//! room's id and life, so the reward settles once wherever it died.
//! Bosses never cross.

use std::time::Duration;

use distributed_game::collide::{bounce, Trace};
use distributed_game::{Ctx, EntityId, PlayerId, Ray, VerifiedPrincipal, ZoneSender};
use game_sim::arena::HALF;
use game_sim::mobpit::{parse_room, room_zone, DOOR_HALF, LASER_MASK, ROOM_SIZE};

use super::{
    dist, pack_hp, step_toward, Killer, LaserHit, Mob, MobPit, MobState, PitEvent, PitMessage,
    BOSS_TIER, BOUNCES, RANGED_DAMAGE, RESPAWN_TICKS,
};

/// A visitor beats this often (ticks, 1 s).
const BEAT_TICKS: u64 = 30;
/// A visitor stays this long past its last renewal (ticks, 2 s).
pub(super) const VISIT_LEASE: u64 = 60;
/// An away slot comes back this long after its last beat (ticks, 4 s):
/// longer than [`VISIT_LEASE`] plus a message's way, so the visitor is
/// always gone first.
pub(super) const AWAY_GRACE: u64 = 120;
/// A mob follows a player who left at most this long (ticks, 5 s).
const FOLLOW_TICKS: u64 = 150;
/// ... and only if it was this close to where the player crossed (m).
const FOLLOW_REACH: f32 = 10.0;
/// A visitor waits this long for the player it came after (ticks, 2 s;
/// the player's own handoff may land after the mob).
const FIND_TICKS: u64 = 60;
/// Visitors a room holds at once.
const MAX_GUESTS: usize = 4;

/// A player who left through a side this activation (recent ones only).
pub(super) struct Departed {
    pub player: PlayerId,
    pub principal: String,
    /// Room offset it went to.
    pub step: (i32, i32),
    /// Where it crossed (room-local).
    pub at: (f32, f32),
    pub tick: u64,
}

/// A mob on its way to where its target left.
pub(super) struct Follow {
    step: (i32, i32),
    /// The exit point, inside the room.
    exit: (f32, f32),
    principal: String,
    until: u64,
}

/// This room's mob, away in `to` (hidden here).
pub(super) struct Away {
    pub to: String,
    pub transfer: u64,
    /// Last beat from its visitor (tick).
    pub beat: u64,
    /// Where it left (room-local): it comes back there.
    pub at: (f32, f32),
}

/// A visitor from room `from`.
pub(super) struct Visit {
    pub from: String,
    pub transfer: u64,
    /// Its id and life in its home room (its kill is keyed by them).
    pub home_id: u32,
    pub generation: u32,
    lease_until: u64,
    next_beat: u64,
    chase: String,
    arrived: u64,
}

/// What a following mob did this tick.
pub(super) enum Followed {
    Moving,
    AtExit,
    GaveUp,
}

/// The rest of a shot past one of the room's sides.
pub(super) struct Leg {
    /// Room offset of the room beyond.
    pub step: (i32, i32),
    /// Where it crosses (that room's coordinates), its direction, the path
    /// and bounces it has left.
    at: [f32; 3],
    dir: [f32; 3],
    dist: f32,
    bounces: u8,
}

impl Leg {
    pub(super) fn message(&self, shot: u16, by: String, age: Duration) -> PitMessage {
        PitMessage::LaserLeg {
            shot,
            by,
            x: self.at[0],
            y: self.at[1],
            z: self.at[2],
            dx: self.dir[0],
            dy: self.dir[1],
            dz: self.dir[2],
            dist: self.dist,
            bounces: self.bounces,
            age_ms: age.as_millis().min(u128::from(u16::MAX)) as u16,
        }
    }
}

/// The first place the beam `origin` → `trace` crosses one of the room's
/// side lines going out, as a leg for the room beyond (`None`: it stayed
/// inside, or has nothing left).
pub(super) fn leaving_leg(
    origin: [f32; 3],
    trace: &Trace,
    range: f32,
    bounces: usize,
) -> Option<Leg> {
    let points: Vec<[f32; 3]> = std::iter::once(origin)
        .chain(trace.hits.iter().map(|h| h.point))
        .chain(std::iter::once(trace.end))
        .collect();
    let mut used = 0.0f32;
    for (k, pair) in points.windows(2).enumerate() {
        let (from, to) = (pair[0], pair[1]);
        let d = [to[0] - from[0], to[1] - from[1], to[2] - from[2]];
        let len = (d[0] * d[0] + d[1] * d[1] + d[2] * d[2]).sqrt();
        if len < 1e-4 {
            continue;
        }
        let dir = [d[0] / len, d[1] / len, d[2] / len];
        let mut best: Option<(f32, (i32, i32))> = None;
        for (axis, sign) in [(0usize, 1.0f32), (0, -1.0), (1, 1.0), (1, -1.0)] {
            let v = dir[axis] * sign;
            // Going out through this side, from inside the room.
            if v <= 1e-6 || from[axis] * sign > HALF {
                continue;
            }
            let t = (HALF - from[axis] * sign) / v;
            if t > len {
                continue;
            }
            let other = from[1 - axis] + dir[1 - axis] * t;
            if other.abs() > HALF || best.is_some_and(|b| b.0 <= t) {
                continue;
            }
            let step = if axis == 0 {
                (sign as i32, 0)
            } else {
                (0, sign as i32)
            };
            best = Some((t, step));
        }
        if let Some((t, step)) = best {
            let left = range - used - t;
            if left < 0.5 {
                return None;
            }
            let at = [
                from[0] + dir[0] * t - step.0 as f32 * ROOM_SIZE,
                from[1] + dir[1] * t - step.1 as f32 * ROOM_SIZE,
                from[2] + dir[2] * t,
            ];
            return Some(Leg {
                step,
                at,
                dir,
                dist: left,
                bounces: bounces.saturating_sub(k).min(usize::from(u8::MAX)) as u8,
            });
        }
        used += len;
    }
    None
}

/// Whether a mob of `radius` gets through the side toward `step` at
/// `along` (room-local, along the side): anywhere on an open side, within
/// the doorway otherwise.
fn passable(open: u8, step: (i32, i32), along: f32, radius: f32) -> bool {
    let bit = match step {
        (1, 0) => 1,
        (-1, 0) => 2,
        (0, 1) => 4,
        _ => 8,
    };
    open & bit != 0 || along.abs() <= DOOR_HALF - radius
}

/// One tick of a mob whose target is gone: follow it to its exit (or, a
/// visitor, wait a moment for the player it came after).
pub(super) fn follow_step(m: &mut Mob, radius: f32, stride: f32, tick: u64) -> Followed {
    let Some(f) = &m.follow else {
        return match &m.visit {
            Some(v) if tick < v.arrived + FIND_TICKS => Followed::Moving,
            _ => Followed::GaveUp,
        };
    };
    if tick > f.until {
        m.follow = None;
        return Followed::GaveUp;
    }
    let (ex, ey) = f.exit;
    m.face = step_toward(&mut m.x, &mut m.y, ex, ey, stride);
    if dist((m.x, m.y), (ex, ey)) < 0.3 + radius * 0.1 {
        Followed::AtExit
    } else {
        Followed::Moving
    }
}

impl MobPit {
    /// Mobs whose target just left through a side follow it (if they were
    /// near and fit through there); visitors find the player they came
    /// after.
    pub(super) fn pick_follows(&mut self, tick: u64, living: &[(PlayerId, f32, f32)]) {
        let open = self.open;
        for m in &mut self.mobs {
            if m.away.is_some() || m.state == MobState::Dead {
                continue;
            }
            let lost = m
                .target
                .is_none_or(|t| !living.iter().any(|(id, _, _)| *id == t));
            if !lost {
                continue;
            }
            if let Some(v) = &m.visit {
                if tick < v.arrived + FIND_TICKS {
                    let chase = v.chase.clone();
                    m.target = self
                        .players
                        .iter()
                        .find(|(id, p)| {
                            p.principal == chase && living.iter().any(|(l, _, _)| l == *id)
                        })
                        .map(|(id, _)| *id);
                    if m.target.is_some() {
                        m.state = MobState::Chase;
                    }
                }
                continue;
            }
            if m.follow.is_some() || m.tier == BOSS_TIER || m.state != MobState::Chase {
                continue;
            }
            let Some(d) = m
                .target
                .and_then(|t| self.departed.iter().find(|d| d.player == t))
            else {
                continue;
            };
            let radius = super::tier_stats(m.tier).3;
            let lim = HALF - radius;
            let exit = (d.at.0.clamp(-lim, lim), d.at.1.clamp(-lim, lim));
            let along = if d.step.0 != 0 { exit.1 } else { exit.0 };
            if dist((m.x, m.y), exit) > FOLLOW_REACH || !passable(open, d.step, along, radius) {
                continue;
            }
            m.follow = Some(Follow {
                step: d.step,
                exit,
                principal: d.principal.clone(),
                until: d.tick + FOLLOW_TICKS,
            });
            m.target = None;
        }
    }

    /// Mob `i` reached the exit its target left by: it crosses (its slot
    /// goes away here; the room beyond takes it as a visitor).
    pub(super) fn start_crossing(&mut self, ctx: &mut Ctx<'_, Self>, i: usize) {
        let Some(f) = self.mobs[i].follow.take() else {
            return;
        };
        self.crossings += 1;
        let transfer = ctx.boot_epoch().rotate_left(20) ^ self.crossings;
        let (rx, ry) = self.room.room;
        let to = room_zone((rx + f.step.0, ry + f.step.1));
        let m = &mut self.mobs[i];
        let radius = super::tier_stats(m.tier).3;
        let lim = HALF - radius;
        // The same world point, just inside the room beyond.
        let x = (m.x - f.step.0 as f32 * ROOM_SIZE).clamp(-lim, lim);
        let y = (m.y - f.step.1 as f32 * ROOM_SIZE).clamp(-lim, lim);
        let message = PitMessage::MobArrive {
            transfer,
            home_id: m.id.get(),
            generation: m.generation,
            tier: m.tier,
            hp: m.hp.max(1) as u32,
            x,
            y,
            chase: f.principal,
        };
        m.away = Some(Away {
            to: to.clone(),
            transfer,
            beat: ctx.tick(),
            at: (m.x, m.y),
        });
        m.state = MobState::Dead;
        m.respawn_at = u64::MAX;
        m.target = None;
        ctx.send_zone(&to, &message);
    }

    /// Visitor `i` is back where it came in: it goes home.
    pub(super) fn visitor_home(&mut self, ctx: &mut Ctx<'_, Self>, i: usize) {
        let m = &mut self.mobs[i];
        let Some(v) = m.visit.take() else {
            return;
        };
        let hp = m.hp.max(1) as u32;
        free(m);
        ctx.send_zone(
            &v.from,
            &PitMessage::MobHome {
                transfer: v.transfer,
                hp,
            },
        );
    }

    /// Mob `i` was just killed: a visitor tells its home.
    pub(super) fn visitor_fell(&mut self, ctx: &mut Ctx<'_, Self>, i: usize) {
        let m = &mut self.mobs[i];
        let Some(v) = m.visit.take() else {
            return;
        };
        free(m);
        ctx.send_zone(
            &v.from,
            &PitMessage::MobFell {
                transfer: v.transfer,
            },
        );
    }

    /// Each tick: visitors beat and lapse, away slots past their grace come
    /// back, old departures are forgotten.
    pub(super) fn keep_crossings(&mut self, ctx: &mut Ctx<'_, Self>) {
        let tick = ctx.tick();
        self.departed.retain(|d| tick <= d.tick + FOLLOW_TICKS);
        let mut beats = Vec::new();
        for m in &mut self.mobs {
            if let Some(v) = &mut m.visit {
                if tick > v.lease_until {
                    // Its home did not renew it: it may have taken it back.
                    m.visit = None;
                    free(m);
                    continue;
                }
                if tick >= v.next_beat {
                    v.next_beat = tick + BEAT_TICKS;
                    beats.push((v.from.clone(), v.transfer));
                }
            }
            if m.away.as_ref().is_some_and(|a| tick > a.beat + AWAY_GRACE) {
                come_back(m);
            }
        }
        for (to, transfer) in beats {
            ctx.send_zone(&to, &PitMessage::MobBeat { transfer });
        }
    }

    /// A message from a room next door (anything else is ignored).
    pub(super) fn take_next_door(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        from: &ZoneSender,
        message: PitMessage,
    ) {
        let (rx, ry) = self.room.room;
        let Some((fx, fy)) = parse_room(&from.zone) else {
            return;
        };
        if rx.abs_diff(fx) + ry.abs_diff(fy) != 1 {
            return;
        }
        let tick = ctx.tick();
        match message {
            PitMessage::LaserLeg {
                shot,
                by,
                x,
                y,
                z,
                dx,
                dy,
                dz,
                dist,
                bounces,
                age_ms,
            } => {
                let Some(principal) = from.principal.clone() else {
                    return;
                };
                // It comes in through the side shared with the sender, inward.
                let step = (rx - fx, ry - fy);
                let (across, along, inward) = if step.0 != 0 {
                    (x * step.0 as f32, y, dx * step.0 as f32)
                } else {
                    (y * step.1 as f32, x, dy * step.1 as f32)
                };
                let finite = [x, y, z, dx, dy, dz, dist].iter().all(|v| v.is_finite());
                if !finite
                    || (across + HALF).abs() > 0.6
                    || along.abs() > HALF + 0.1
                    || inward <= 0.0
                {
                    return;
                }
                self.resolve_leg(
                    ctx,
                    principal,
                    shot,
                    by,
                    [x, y, z],
                    [dx, dy, dz],
                    dist.min(super::RANGE),
                    bounces.min(BOUNCES as u8),
                    age_ms,
                );
            }
            PitMessage::MobArrive {
                transfer,
                home_id,
                generation,
                tier,
                hp,
                x,
                y,
                chase,
            } => {
                if self.mobs.iter().any(|m| {
                    m.visit
                        .as_ref()
                        .is_some_and(|v| v.from == from.zone && v.transfer == transfer)
                }) {
                    return;
                }
                let slot = self
                    .mobs
                    .iter()
                    .position(|m| m.guest && m.visit.is_none() && m.state == MobState::Dead);
                let guests = self.mobs.iter().filter(|m| m.guest).count();
                if tier == BOSS_TIER
                    || !(x.is_finite() && y.is_finite())
                    || (slot.is_none() && guests >= MAX_GUESTS)
                {
                    ctx.send_zone(&from.zone, &PitMessage::MobRecall { transfer });
                    return;
                }
                let i = slot.unwrap_or_else(|| {
                    let id = EntityId(self.mobs.len() as u32 + 1);
                    self.mobs.push(guest_slot(id));
                    self.mobs.len() - 1
                });
                let target = self
                    .players
                    .iter()
                    .find(|(_, p)| p.principal == chase)
                    .map(|(id, _)| *id);
                let max = pack_hp(tier, self.world.hood_tier, &self.pack);
                let radius = super::tier_stats(tier).3;
                let lim = HALF - radius;
                let (x, y) = (x.clamp(-lim, lim), y.clamp(-lim, lim));
                let m = &mut self.mobs[i];
                m.tier = tier;
                m.hp = (hp as i32).clamp(1, max.max(1));
                (m.x, m.y) = (x, y);
                m.home = (x, y);
                m.wander = (x, y);
                m.face = (-y).atan2(-x);
                m.state = MobState::Chase;
                m.target = target;
                m.timer = 0;
                m.visit = Some(Visit {
                    from: from.zone.clone(),
                    transfer,
                    home_id,
                    generation,
                    lease_until: tick + VISIT_LEASE,
                    next_beat: tick + BEAT_TICKS,
                    chase,
                    arrived: tick,
                });
            }
            PitMessage::MobBeat { transfer } => {
                let away = self.mobs.iter_mut().find_map(|m| {
                    m.away
                        .as_mut()
                        .filter(|a| a.to == from.zone && a.transfer == transfer)
                });
                let reply = match away {
                    Some(a) => {
                        a.beat = tick;
                        PitMessage::MobLease { transfer }
                    }
                    None => PitMessage::MobRecall { transfer },
                };
                ctx.send_zone(&from.zone, &reply);
            }
            PitMessage::MobLease { transfer } => {
                if let Some(v) = self.visitor(&from.zone, transfer) {
                    v.lease_until = tick + VISIT_LEASE;
                }
            }
            PitMessage::MobRecall { transfer } => {
                for m in &mut self.mobs {
                    if m.visit
                        .as_ref()
                        .is_some_and(|v| v.from == from.zone && v.transfer == transfer)
                    {
                        m.visit = None;
                        free(m);
                    }
                    if m.away
                        .as_ref()
                        .is_some_and(|a| a.to == from.zone && a.transfer == transfer)
                    {
                        come_back(m);
                    }
                }
            }
            PitMessage::MobHome { transfer, hp } => {
                if let Some(m) = self.away_mob(&from.zone, transfer) {
                    come_back(m);
                    m.hp = m.hp.min(hp as i32).max(1);
                }
            }
            PitMessage::MobFell { transfer } => {
                if let Some(m) = self.away_mob(&from.zone, transfer) {
                    m.away = None;
                    m.hp = 0;
                    m.state = MobState::Dead;
                    m.respawn_at = tick + RESPAWN_TICKS;
                }
            }
            _ => {}
        }
    }

    fn visitor(&mut self, from: &str, transfer: u64) -> Option<&mut Visit> {
        self.mobs.iter_mut().find_map(|m| {
            m.visit
                .as_mut()
                .filter(|v| v.from == from && v.transfer == transfer)
        })
    }

    fn away_mob(&mut self, to: &str, transfer: u64) -> Option<&mut Mob> {
        self.mobs.iter_mut().find(|m| {
            m.away
                .as_ref()
                .is_some_and(|a| a.to == to && a.transfer == transfer)
        })
    }

    /// The rest of a shot from next door, as old as the shooter's view: it
    /// may hit a mob here (credited to the shooter) or carry on again.
    #[allow(clippy::too_many_arguments)]
    fn resolve_leg(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        principal: VerifiedPrincipal,
        shot: u16,
        by: String,
        at: [f32; 3],
        dir: [f32; 3],
        dist: f32,
        bounces: u8,
        age_ms: u16,
    ) {
        let (time, oldest) = {
            let rewind = ctx.rewind();
            let Some(latest) = rewind.latest().map(|f| f.time()) else {
                return;
            };
            let oldest = rewind.oldest().map_or(latest, |f| f.time());
            (
                latest.saturating_sub(Duration::from_millis(age_ms.into())),
                oldest,
            )
        };
        let time = time.max(oldest);
        // Just inside the shared side (a wall face there would turn it back).
        let start = [
            at[0] + dir[0] * 0.01,
            at[1] + dir[1] * 0.01,
            at[2] + dir[2] * 0.01,
        ];
        let ray = Ray::new(start, dir, dist).mask(LASER_MASK);
        let trace = {
            let mobs = &self.mobs;
            bounce(ray, usize::from(bounces), |leg| {
                ctx.raycast_where(time, leg, |id| {
                    mobs.get(id as usize - 1)
                        .is_some_and(|m| m.state != MobState::Dead)
                })
            })
        };
        let hit = trace.entity();
        let leaves = if hit.is_none() {
            leaving_leg(start, &trace, dist, usize::from(bounces))
        } else {
            None
        };
        let path = trace
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
        ctx.broadcast(&PitEvent::Beam {
            shot,
            by: by.clone(),
            x: at[0],
            y: at[1],
            z: at[2],
            path,
            end_x: trace.end[0],
            end_y: trace.end[1],
            end_z: trace.end[2],
            hit,
        });
        if let Some(id) = hit {
            self.hurt_mob(
                ctx,
                Killer::Next {
                    principal: principal.clone(),
                    name: by.clone(),
                },
                id as usize - 1,
                RANGED_DAMAGE,
            );
        }
        if let Some(leg) = leaves {
            let (rx, ry) = self.room.room;
            let to = room_zone((rx + leg.step.0, ry + leg.step.1));
            let age = Duration::from_millis(age_ms.into());
            ctx.send_zone_as(&principal, &to, &leg.message(shot, by, age));
        }
    }
}

/// An empty guest slot (beyond the room's own mobs).
fn guest_slot(id: EntityId) -> Mob {
    Mob {
        id,
        tier: 1,
        home: (0.0, 0.0),
        x: 0.0,
        y: 0.0,
        face: 0.0,
        hp: 0,
        state: MobState::Dead,
        timer: 0,
        target: None,
        generation: 0,
        respawn_at: u64::MAX,
        wander: (0.0, 0.0),
        difficulty: 0,
        hp_pct: 100,
        guest: true,
        visit: None,
        away: None,
        follow: None,
    }
}

/// A guest slot gives its visitor up.
fn free(m: &mut Mob) {
    m.state = MobState::Dead;
    m.hp = 0;
    m.target = None;
    m.follow = None;
    m.respawn_at = u64::MAX;
}

/// An away slot's mob is back home, where it left, calm.
fn come_back(m: &mut Mob) {
    if let Some(a) = m.away.take() {
        (m.x, m.y) = a.at;
        m.state = MobState::Return;
        m.respawn_at = 0;
        m.target = None;
        m.follow = None;
    }
}
