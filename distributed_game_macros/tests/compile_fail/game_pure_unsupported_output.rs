use distributed_game::game_pure;

struct Opaque;

// The output must have a payload encoding.
#[game_pure(Make)]
fn make() -> Opaque {
    Opaque
}

fn main() {}
