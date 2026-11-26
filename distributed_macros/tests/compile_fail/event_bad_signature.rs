use distributed::{sourced, Entity};

struct Todo {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "todo")]
impl Todo {
    // `#[event]` methods are replayed as `self.method(...)`, so they must take
    // a `self` receiver. A free associated function is unsupported.
    #[event("initialized")]
    pub fn initialize(id: String) {
        let _ = id;
    }
}

fn main() {}
