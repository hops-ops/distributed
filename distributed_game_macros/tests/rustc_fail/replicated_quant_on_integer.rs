use distributed_game::Replicated;

#[derive(Replicated)]
#[replicated(kind = "orb", id = 1)]
struct OrbView {
    #[quant(min = 0.0, max = 10.0, bits = 8)]
    x: u8,
    y: f32,
}

fn main() {}
