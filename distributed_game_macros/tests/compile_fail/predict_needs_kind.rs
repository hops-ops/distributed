use distributed_game::{predict, GameInput, GameRecord};

#[derive(Clone, Copy, GameRecord)]
struct State {
    x: f32,
}

#[derive(Clone, Copy, GameInput)]
struct Push {
    dx: i8,
}

#[predict(Step)]
fn step(state: &State, _input: &Push, _context: &(), _dt: f32) -> State {
    *state
}

fn main() {}
