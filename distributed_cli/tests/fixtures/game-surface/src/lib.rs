//! A minimal zone surface for `distributed game-manifest` tests.

use distributed_game::{
    game_pure, predict, schema, Ctx, EntitySchema, GameEvent, GameInput, GameRecord, GameSurface, PlayerId,
    Replicated, WorldView, Zone,
};

#[derive(Replicated)]
#[replicated(kind = "orb", id = 1)]
pub struct OrbView {
    #[quant(centered = 32.0, bits = 14)]
    #[correct(ease = 80)]
    pub x: f32,
    #[quant(centered = 32.0, bits = 14)]
    #[correct(ease = 80)]
    pub y: f32,
}

#[derive(Clone, Copy, GameInput)]
pub struct Steer {
    #[range(min = -127, max = 127)]
    pub turn: i8,
    #[buttons(boost)]
    pub buttons: u8,
}

#[derive(GameEvent)]
pub enum OrbEvent {
    #[game_event(id = 1)]
    Hello { entity: u32 },
}

/// The predicted part of an orb.
#[derive(Clone, Copy, GameRecord)]
pub struct OrbState {
    pub x: f32,
}

/// The client-predicted step (compiled to WASM by `distributed game-wasm`).
#[predict(OrbStep, kind = "orb")]
pub fn orb_step(state: &OrbState, input: &Steer, _context: &(), dt: f32) -> OrbState {
    OrbState {
        x: state.x + f32::from(input.turn) * dt,
    }
}

/// A pure function compiled into the same module (`dg_pure_orb_radius`).
#[game_pure(OrbRadiusFn)]
pub fn orb_radius(state: &OrbState) -> f32 {
    (state.x * state.x).sqrt()
}

/// Static data the client gets as a generated constant.
#[derive(Clone, Copy, GameRecord)]
pub struct OrbLimits {
    pub limit: f32,
}

pub struct Orbs;

impl Zone for Orbs {
    type Config = ();
    type Input = Steer;
    type Timer = ();

    fn schema() -> EntitySchema {
        schema![OrbView]
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, _config: ()) -> Self {
        Orbs
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<Steer> {
        Steer::from_payload(payload)
    }

    fn on_tick(&mut self, _ctx: &mut Ctx<'_, Self>) {}

    fn replicate(&self, out: &mut WorldView) {
        out.add(1, &OrbView { x: 0.0, y: 0.0 });
    }
}

/// The export `distributed game-manifest` runs by default.
pub fn game_surface() -> GameSurface {
    GameSurface::for_zone::<Orbs>("orbs")
        .events::<OrbEvent>()
        .predict::<OrbStep>()
        .pure::<OrbRadiusFn>()
        .constant("orb_limits", &OrbLimits { limit: 32.0 })
}
