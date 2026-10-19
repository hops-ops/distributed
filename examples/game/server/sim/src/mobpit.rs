//! Mob Pit's input frame and its client-predicted step (the Arena's
//! movement; combat is server-owned and never predicted).

use distributed_game::{predict, GameInput};

use crate::arena::{self, MoveInput, MoveState};

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

/// The own capsule's predicted step: the Arena's movement of
/// [`PitInput::movement`].
#[predict(PitStep, kind = "player")]
pub fn pit_step(state: &MoveState, input: &PitInput, _context: &(), _dt: f32) -> MoveState {
    arena::step(*state, input.movement())
}
