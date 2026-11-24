//! The unit tests' zone.

use crate::{
    Ctx, EntitySchema, EntityState, FieldType, FieldValue, KindDef, PlayerId, WorldView, Zone,
};

/// `n` dots (its config) on the x axis whose value changes every tick.
pub(crate) struct Dots {
    n: u32,
    ticks: u32,
}

impl Zone for Dots {
    type Config = u32;
    type Input = u8;
    type Timer = ();
    type Message = crate::NoMessages;

    fn schema() -> EntitySchema {
        EntitySchema::new().kind(KindDef::new(1, "dot").field("v", FieldType::U16))
    }

    fn on_create(_ctx: &mut Ctx<'_, Self>, n: u32) -> Self {
        Self { n, ticks: 0 }
    }

    fn decode_input(&self, _player: PlayerId, payload: &[u8]) -> Option<u8> {
        payload.first().copied()
    }

    fn on_tick(&mut self, _ctx: &mut Ctx<'_, Self>) {
        self.ticks += 1;
    }

    fn replicate(&self, out: &mut WorldView) {
        for i in 0..self.n {
            out.push(EntityState::new(
                i,
                1,
                i as f32,
                0.0,
                vec![FieldValue::U(self.ticks + i)],
            ));
        }
    }
}
