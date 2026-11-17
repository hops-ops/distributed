use distributed_game::{predict, GameInput, GameRecord};

#[derive(Clone, Copy, GameRecord)]
struct State {
    x: f32,
}

#[derive(Clone, Copy, GameInput)]
struct Push {
    dx: i8,
}

// The state must be borrowed, and the frame length is a trailing `dt: f32`.
#[predict(Step, kind = "dot")]
fn step(state: State, _input: &Push) -> State {
    state
}

fn main() {}
