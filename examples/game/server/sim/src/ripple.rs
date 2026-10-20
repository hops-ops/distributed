//! Boss Ripple's hood input and its client-predicted step (the Arena's
//! movement; attacks are server-owned).

use distributed_game::{predict, GameInput};

use crate::arena::{self, MoveInput, MoveState};

/// Hood input: Arena movement + action bits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, GameInput)]
pub struct HoodInput {
    /// Stick x.
    #[range(min = -127, max = 127)]
    pub mx: i8,
    /// Stick y.
    #[range(min = -127, max = 127)]
    pub my: i8,
    /// [`HoodInput::DASH`].
    #[buttons(dash)]
    pub buttons: u8,
    /// [`HoodInput::ATTACK`] | [`HoodInput::SMITE`].
    #[buttons(attack, smite)]
    pub action: u8,
}

impl HoodInput {
    /// The movement part (the Arena's step).
    pub fn movement(self) -> MoveInput {
        MoveInput {
            mx: self.mx,
            my: self.my,
            buttons: self.buttons,
        }
    }
}

/// A fighter's predicted step: the Arena's movement of
/// [`HoodInput::movement`].
#[predict(HoodStep, kind = "player")]
pub fn hood_step(state: &MoveState, input: &HoodInput, _context: &(), _dt: f32) -> MoveState {
    arena::step(*state, input.movement())
}
