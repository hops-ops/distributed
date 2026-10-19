//! A minimal zone surface for `distributed game-manifest` tests.

use distributed_game::{
    schema, Ctx, EntitySchema, GameEvent, GameInput, GameSurface, PlayerId, Replicated, WorldView,
    Zone,
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
    GameSurface::for_zone::<Orbs>("orbs").events::<OrbEvent>()
}
