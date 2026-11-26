use distributed::{sourced, Entity};

#[derive(Default)]
struct Todo {
    entity: Entity,
}

#[sourced(entity)]
impl Todo {
    #[event("todo.purged", domain = deleted)]
    fn purge(&mut self) {}
}

fn main() {}
