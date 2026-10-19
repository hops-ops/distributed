use distributed_game::GameEvent;

#[derive(GameEvent)]
enum ZoneEvent {
    #[game_event(id = 1)]
    Hello { entity: u32 },
    #[game_event(id = 1)]
    Bye,
}

fn main() {}
