use distributed_game::Replicated;

struct State {
    x: f32,
    y: f32,
}

struct Player {
    state: State,
    hp: i32,
}

#[derive(Replicated)]
#[replicated(kind = "player", id = 1, of = Player)]
struct PlayerView {
    #[from(state.x)]
    x: f32,
    #[from(state.y)]
    y: f32,
    #[from]
    hp: u8,
}

fn main() {
    let p = Player { state: State { x: 0.0, y: 0.0 }, hp: 1 };
    let _ = (p.state.x, p.state.y, p.hp);
}
