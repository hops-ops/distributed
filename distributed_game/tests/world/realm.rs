//! Test zone for world-structure tests: handoff, tier-up events,
//! hibernation, gates and freeze readiness.

#![allow(dead_code)]

use distributed_game::*;

/// Input commands (first payload byte).
pub const CMD_HANDOFF: u8 = 1;
pub const CMD_BOSS: u8 = 2;
pub const CMD_COUNT: u8 = 3;

/// Event type ids sent to players.
pub const EV_JOINED: u32 = 1;
pub const EV_ARRIVED: u32 = 2;

#[derive(Debug, Clone, Default)]
pub struct RealmConfig {
    pub gate: bool,
    pub freeze_ready: bool,
    pub neighbour: Option<String>,
    pub reject_all: bool,
}

impl RealmConfig {
    pub fn new() -> Self {
        Self {
            freeze_ready: true,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arrival {
    pub principal: String,
    pub from: String,
    pub carried: Vec<u8>,
}

pub struct Realm {
    pub cfg: RealmConfig,
    pub counter: u32,
    pub restored_from: Option<String>,
    pub arrivals: Vec<Arrival>,
    pub child_events: Vec<ChildEvent>,
    pub left: Vec<(PlayerId, LeaveReason)>,
    pub versions: Vec<(String, String)>,
    pub handoff_errors: Vec<HandoffError>,
}

impl Zone for Realm {
    type Config = RealmConfig;
    type Input = u8;
    type Timer = ();

    fn schema() -> EntitySchema {
        EntitySchema::new().kind(KindDef::new(1, "realm").field("counter", FieldType::U32))
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, cfg: RealmConfig) -> Self {
        Self {
            cfg,
            counter: 0,
            restored_from: None,
            arrivals: Vec::new(),
            child_events: Vec::new(),
            left: Vec::new(),
            versions: Vec::new(),
            handoff_errors: Vec::new(),
        }
    }

    fn on_auth(&mut self, _ctx: &mut Ctx<'_, Self>, _req: &JoinRequest<'_>) -> Admission {
        if self.cfg.reject_all {
            Admission::Reject("closed for repairs".into())
        } else {
            Admission::Admit
        }
    }

    fn on_join(&mut self, ctx: &mut Ctx<'_, Self>, player: PlayerId) {
        ctx.send_event(player, EV_JOINED, self.counter.to_le_bytes().to_vec());
    }

    fn on_handoff_join(
        &mut self,
        ctx: &mut Ctx<'_, Self>,
        player: PlayerId,
        from_zone: &str,
        carried: Vec<u8>,
    ) {
        let principal = ctx
            .player(player)
            .map(|p| p.principal.clone())
            .unwrap_or_default();
        let mut payload = carried.clone();
        payload.extend_from_slice(b"|");
        payload.extend_from_slice(from_zone.as_bytes());
        ctx.send_event(player, EV_ARRIVED, payload);
        self.arrivals.push(Arrival {
            principal,
            from: from_zone.to_string(),
            carried,
        });
    }

    fn on_leave(&mut self, _ctx: &mut Ctx<'_, Self>, player: PlayerId, reason: LeaveReason) {
        self.left.push((player, reason));
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<u8> {
        payload.first().copied()
    }

    fn on_tick(&mut self, ctx: &mut Ctx<'_, Self>) {
        let players: Vec<_> = ctx.players().map(|p| (p.id, p.principal.clone())).collect();
        for (player, principal) in players {
            for input in ctx.take_inputs(player, ConsumePolicy::Drain) {
                match input.input {
                    CMD_HANDOFF => {
                        let Some(target) = self.cfg.neighbour.clone() else {
                            continue;
                        };
                        let carried = format!("{principal}:gold={}", self.counter).into_bytes();
                        if let Err(e) = ctx.handoff(player, &target, carried) {
                            self.handoff_errors.push(e);
                        }
                    }
                    CMD_BOSS => {
                        let zone = ctx.zone_id().to_string();
                        ctx.emit_upward("boss_defeated", zone.into_bytes(), ChildEvent::TO_ROOT);
                    }
                    CMD_COUNT => {
                        self.counter += 1;
                        let key = format!("count:{}:{}", ctx.zone_id(), self.counter);
                        let _ = ctx.emit_fact_for(
                            player,
                            Fact::new(key, "realm.count", serde_json::json!({"n": self.counter})),
                        );
                    }
                    _ => {}
                }
            }
        }
    }

    fn replicate(&self, out: &mut WorldView) {
        out.push(EntityState::new(1, 1, 0.0, 0.0, vec![FieldValue::U(self.counter)]).global());
    }

    fn on_dispose(&mut self, ctx: &mut Ctx<'_, Self>) {
        let key = format!("dispose:{}:{}", ctx.zone_id(), self.counter);
        let _ = ctx.emit_fact(Fact::new(key, "realm.disposed", serde_json::json!({})));
    }

    fn hibernate(&self) -> Option<Vec<u8>> {
        Some(self.counter.to_le_bytes().to_vec())
    }

    fn on_restore(&mut self, _ctx: &mut Ctx<'_, Self>, state: &[u8], saved_version: &str) {
        if let Ok(bytes) = <[u8; 4]>::try_from(state) {
            self.counter = u32::from_le_bytes(bytes);
            self.restored_from = Some(saved_version.to_string());
        }
    }

    fn on_child_event(&mut self, _ctx: &mut Ctx<'_, Self>, event: &ChildEvent) {
        self.child_events.push(event.clone());
    }

    fn is_gate(&self) -> bool {
        self.cfg.gate
    }

    fn freeze_ready(&self) -> bool {
        self.cfg.freeze_ready
    }

    fn on_content_version_changed(&mut self, _ctx: &mut Ctx<'_, Self>, from: &str, to: &str) {
        self.versions.push((from.to_string(), to.to_string()));
    }
}
