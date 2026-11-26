use distributed::domain_event::{
    DomainEvent, DomainEventBodyContract, DomainEventBodyDescriptor, DomainEventBodyKind,
    DomainEventContract, DomainEventDescriptor,
};
use distributed::{sourced, Entity};
use serde::Serialize;

#[derive(Serialize)]
struct TodoCompleted;

impl DomainEvent for TodoCompleted {
    const DESCRIPTOR: DomainEventDescriptor = DomainEventDescriptor {
        name: std::borrow::Cow::Borrowed("todo.completed"),
        version: 1,
        body: DomainEventBodyDescriptor::distributed_json(
            DomainEventBodyKind::Event,
            "TodoCompleted",
            1,
            "todo-completed-v1",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        ),
    };
}

impl DomainEventContract for TodoCompleted {
    const EVENT_NAME: &'static str = "todo.lied";
    const EVENT_VERSION: u64 = 1;

    fn descriptor() -> DomainEventDescriptor {
        Self::DESCRIPTOR.clone()
    }
}

impl DomainEventBodyContract<Self> for TodoCompleted {}

#[derive(Default)]
struct Todo {
    entity: Entity,
}

fn capture(_todo: &Todo, _event: &TodoReplayEvent) -> TodoCompleted {
    TodoCompleted
}

#[sourced(entity, events = "TodoReplayEvent", aggregate_type = "todo")]
impl Todo {
    #[event("todo.completed", domain = with(TodoCompleted, capture))]
    fn complete(&mut self) {}
}

fn main() {}
