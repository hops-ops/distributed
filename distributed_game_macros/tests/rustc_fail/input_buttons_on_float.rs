use distributed_game::GameInput;

#[derive(GameInput)]
struct PadInput {
    #[buttons(jump, duck)]
    buttons: f32,
}

fn main() {}
