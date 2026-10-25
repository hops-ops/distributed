use distributed_game::{predict, GameInput, GameRecord};

#[derive(Clone, Copy, GameRecord)]
struct State {
    x: f32,
}

#[derive(Clone, Copy, GameInput)]
struct Push {
    dx: i8,
}

#[predict(Step, kind = "dot")]
fn step(state: &State, _input: &Push, _context: &(), _dt: f32) -> f32 {
    state.x
}

fn main() {}
