use distributed::{sourced, Entity};

struct Todo {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "todo")]
impl Todo {
    #[event("done")]
    pub fn complete(&mut self) {}

    // Same event name as `complete` — ambiguous on replay, rejected up front.
    #[event("done")]
    pub fn finish(&mut self) {}
}

fn main() {}
