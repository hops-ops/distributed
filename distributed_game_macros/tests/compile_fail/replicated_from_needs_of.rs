use distributed_game::Replicated;

#[derive(Replicated)]
#[replicated(kind = "mob", id = 2)]
struct MobView {
    #[from]
    x: f32,
    y: f32,
}

fn main() {}
