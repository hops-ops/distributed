use distributed::{sourced, Entity, SourcedResult};

#[derive(Default)]
struct Todo {
    entity: Entity,
}

impl Todo {
    fn validate(&self) -> SourcedResult {
        Ok(())
    }
}

#[sourced(entity, aggregate_type = "todo")]
impl Todo {
    #[event("todo.completed")]
    fn complete(&mut self) {
        self.validate()?;
    }
}

fn main() {}
