use distributed_game::Replicated;

#[derive(Replicated)]
#[replicated(kind = "orb", id = 1)]
struct OrbView {
    #[correct(wobble)]
    x: f32,
    y: f32,
}

fn main() {}
