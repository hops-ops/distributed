use distributed_game::Replicated;

#[derive(Replicated)]
#[replicated(kind = "orb", id = 1)]
struct OrbView {
    x: f32,
    y: f32,
    charge: i8,
}

fn main() {}
