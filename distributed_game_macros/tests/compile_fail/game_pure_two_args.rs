use distributed_game::game_pure;

// One argument at most: bundle several values in a #[derive(GameRecord)] struct.
#[game_pure(Sum)]
fn sum(a: &u32, b: &u32) -> u32 {
    a + b
}

fn main() {}
