use distributed_game::Replicated;

#[derive(Replicated)]
#[replicated(kind = "score", id = 3)]
struct ScoreView {
    points: u32,
}

fn main() {}
