//! Shared test zone: avatars move by input, coins spawn on a timer, picking a
//! coin emits a durable fact credited to the verified principal.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use distributed_game::{
    Admission, ConsumePolicy, Ctx, EntitySchema, EntityState, Fact, FactSettlement, FieldType,
    FieldValue, Fnv64, JoinRequest, KindDef, LeaveReason, ObserverId, PlayerId, Tier, TimerId,
    View, WorldView, Zone,
};
use serde_json::json;

pub const AVATAR: u16 = 1;
pub const COIN: u16 = 2;

#[derive(Debug, Clone, Default)]
pub struct ArenaConfig {
    pub fact_every_tick: bool,
    pub reject_principal: Option<String>,
    pub background: bool,
    pub spawn_every: Option<Duration>,
    /// Allocation payload handed over by the zone factory.
    pub allocation: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct Avatar {
    pub entity: u32,
    pub x: f32,
    pub y: f32,
    pub score: u32,
}

#[derive(Debug, Clone)]
pub enum ArenaTimer {
    SpawnCoin,
}

pub struct Arena {
    pub cfg: ArenaConfig,
    pub avatars: BTreeMap<PlayerId, Avatar>,
    pub coins: BTreeMap<u32, (f32, f32)>,
    pub next_entity: u32,
    pub log: Vec<String>,
    pub settled: Vec<FactSettlement>,
    pub emit_errors: Vec<distributed_game::EmitError>,
    pub ticks: u64,
    pub timer_fires: u64,
    pub tiers: Vec<(Tier, Tier)>,
    /// Extension claims seen by `on_auth`.
    pub auth_extensions: Vec<distributed_game::TicketExtensions>,
}

impl Arena {
    pub fn state_hash(&self) -> u64 {
        let mut h = Fnv64::new();
        for (id, a) in &self.avatars {
            h.write_u64(u64::from(id.0))
                .write_f32(a.x)
                .write_f32(a.y)
                .write_u64(u64::from(a.score));
        }
        for (id, (x, y)) in &self.coins {
            h.write_u64(u64::from(*id)).write_f32(*x).write_f32(*y);
        }
        h.write_u64(self.ticks);
        h.finish()
    }

    fn spawn_coin(&mut self, ctx: &mut Ctx<'_, Self>) {
        let x = ctx.rng().next_f32() * 100.0;
        let y = ctx.rng().next_f32() * 100.0;
        self.next_entity += 1;
        self.coins.insert(self.next_entity, (x, y));
    }
}

impl Zone for Arena {
    type Config = ArenaConfig;
    type Input = (i8, i8);
    type Timer = ArenaTimer;

    fn schema() -> EntitySchema {
        let pos = FieldType::QuantF32 {
            min: 0.0,
            max: 100.0,
            bits: 16,
        };
        EntitySchema::new()
            .kind(
                KindDef::new(AVATAR, "avatar")
                    .field("x", pos)
                    .field("y", pos)
                    .field("score", FieldType::U16),
            )
            .kind(
                KindDef::new(COIN, "coin")
                    .field("x", pos)
                    .field("y", pos)
                    .priority(0.5),
            )
    }

    fn on_create(ctx: &mut Ctx<'_, Self>, cfg: ArenaConfig) -> Self {
        if let Some(every) = cfg.spawn_every {
            ctx.set_interval(every, ArenaTimer::SpawnCoin);
        }
        Self {
            cfg,
            avatars: BTreeMap::new(),
            coins: BTreeMap::new(),
            next_entity: 1000,
            log: vec!["create".into()],
            settled: Vec::new(),
            emit_errors: Vec::new(),
            ticks: 0,
            timer_fires: 0,
            tiers: Vec::new(),
            auth_extensions: Vec::new(),
        }
    }

    fn on_auth(&mut self, _ctx: &mut Ctx<'_, Self>, req: &JoinRequest<'_>) -> Admission {
        self.auth_extensions.push(req.claims.extensions.clone());
        if self.cfg.reject_principal.as_deref() == Some(req.claims.principal.as_str()) {
            return Admission::Reject("banned".into());
        }
        Admission::Admit
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        let x = 10.0 + ctx.rng().next_f32() * 80.0;
        let y = 10.0 + ctx.rng().next_f32() * 80.0;
        self.avatars.insert(
            player,
            Avatar {
                entity: player.0,
                x,
                y,
                score: 0,
            },
        );
        ctx.set_view(player, View::new(x, y, 30.0));
        let principal = ctx.player(player).unwrap().principal.clone();
        self.log.push(format!("join:{principal}"));
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, reason: LeaveReason) {
        self.avatars.remove(&player);
        self.log.push(format!("leave:{reason:?}"));
    }

    fn on_observe(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId) {
        ctx.set_view(observer, View::new(50.0, 50.0, 1000.0));
        let info = ctx
            .observer(observer)
            .expect("observer known in on_observe");
        let line = format!(
            "observe:{}:{}",
            info.principal,
            info.name.as_deref().unwrap_or("-")
        );
        let mine: Vec<_> = ctx.observers_of(&info.principal.clone()).collect();
        assert_eq!(mine, vec![observer]);
        ctx.send_event(observer, 7, line.clone().into_bytes());
        self.log.push(line);
    }

    fn on_unobserve(&mut self, ctx: &mut Ctx<'_, Self>, observer: ObserverId) {
        let principal = ctx.observer(observer).map(|o| o.principal.clone());
        self.log
            .push(format!("unobserve:{}", principal.as_deref().unwrap_or("?")));
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<(i8, i8)> {
        match payload {
            [dx, dy] => Some((*dx as i8, *dy as i8)),
            _ => None,
        }
    }

    fn sanitize_input(&self, _player: PlayerId, (dx, dy): (i8, i8)) -> Option<(i8, i8)> {
        if dx.unsigned_abs() > 4 || dy.unsigned_abs() > 4 {
            return None;
        }
        Some((dx.clamp(-1, 1), dy.clamp(-1, 1)))
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        self.ticks += 1;
        let dt = ctx.dt_secs();
        let players: Vec<PlayerId> = self.avatars.keys().copied().collect();
        for player in players {
            let inputs = ctx.take_inputs(player, ConsumePolicy::Drain);
            let avatar = self.avatars.get_mut(&player).unwrap();
            for input in inputs {
                avatar.x = (avatar.x + f32::from(input.input.0) * 10.0 * dt).clamp(0.0, 100.0);
                avatar.y = (avatar.y + f32::from(input.input.1) * 10.0 * dt).clamp(0.0, 100.0);
            }
            let (ax, ay) = (avatar.x, avatar.y);
            let picked: Vec<u32> = self
                .coins
                .iter()
                .filter(|(_, (x, y))| (x - ax).powi(2) + (y - ay).powi(2) < 4.0)
                .map(|(id, _)| *id)
                .collect();
            for coin in picked {
                self.coins.remove(&coin);
                self.avatars.get_mut(&player).unwrap().score += 1;
                let fact = Fact::new(
                    format!("coin:{}:{coin}", ctx.zone_id()),
                    "credit_xp",
                    json!({ "amount": 10 }),
                );
                if let Err(e) = ctx.emit_fact_for(player, fact) {
                    self.emit_errors.push(e);
                }
            }
            ctx.set_view(player, View::new(ax, ay, 30.0));
        }
        if self.cfg.fact_every_tick {
            let fact = Fact::new(format!("tick:{}", ctx.tick()), "heartbeat", json!(null));
            if let Err(e) = ctx.emit_fact(fact) {
                self.emit_errors.push(e);
            }
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx<'_, Self>, _id: TimerId, timer: ArenaTimer) {
        self.timer_fires += 1;
        match timer {
            ArenaTimer::SpawnCoin => self.spawn_coin(ctx),
        }
    }

    fn on_fact_settled(&mut self, _ctx: &mut Ctx<'_, Self>, settlement: FactSettlement) {
        self.settled.push(settlement);
    }

    fn on_tier_changed(&mut self, _ctx: &mut Ctx<'_, Self>, from: Tier, to: Tier) {
        self.tiers.push((from, to));
    }

    fn wants_background(&self) -> bool {
        self.cfg.background
    }

    fn replicate(&self, out: &mut WorldView) {
        for a in self.avatars.values() {
            out.push(EntityState::new(
                a.entity,
                AVATAR,
                a.x,
                a.y,
                vec![
                    FieldValue::F(a.x),
                    FieldValue::F(a.y),
                    FieldValue::U(a.score),
                ],
            ));
        }
        for (id, (x, y)) in &self.coins {
            out.push(EntityState::new(
                *id,
                COIN,
                *x,
                *y,
                vec![FieldValue::F(*x), FieldValue::F(*y)],
            ));
        }
    }

    fn on_dispose(&mut self, _ctx: &mut Ctx<'_, Self>) {
        self.log.push("dispose".into());
    }
}
