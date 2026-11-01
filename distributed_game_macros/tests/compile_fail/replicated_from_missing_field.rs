use distributed_game::Replicated;

struct Mob {
    x: f32,
    y: f32,
}

#[derive(Replicated)]
#[replicated(kind = "mob", id = 2, of = Mob)]
struct MobView {
    #[from]
    x: f32,
    #[from]
    y: f32,
    #[from]
    tier: u8,
}

fn main() {
    let _ = Mob { x: 0.0, y: 0.0 }.x;
}
