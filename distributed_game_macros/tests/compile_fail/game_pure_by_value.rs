use distributed_game::game_pure;

// The argument is borrowed.
#[game_pure(Double)]
fn double(a: u32) -> u32 {
    a * 2
}

fn main() {}
