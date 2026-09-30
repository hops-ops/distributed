//! Minimal Counter aggregate used as the load-test write-path fixture.

use distributed::{sourced, Entity, OutboxMessage, Snapshot, SourcedResult};
use serde::{Deserialize, Serialize};

#[derive(Default, Snapshot)]
pub struct Counter {
    pub entity: Entity,
    pub value: i64,
}

#[sourced(entity, aggregate_type = "load.counter")]
impl Counter {
    #[event("initialized")]
    pub fn create(&mut self, id: String) {
        self.entity.set_id(&id);
        self.value = 0;
    }

    #[event("incremented")]
    pub fn increment(&mut self, amount: i64) {
        self.value += amount;
    }
}

#[derive(Serialize, Deserialize)]
pub struct CreateCounter {
    pub id: String,
}

#[derive(Serialize, Deserialize)]
pub struct IncrementCounter {
    pub id: String,
    pub amount: i64,
}

#[derive(Serialize)]
struct CounterState<'a> {
    id: &'a str,
    value: i64,
}

pub fn counter_state_message(counter: &Counter, event_type: &str) -> SourcedResult<OutboxMessage> {
    OutboxMessage::encode_for_entity(
        format!(
            "{}:{event_type}:{}",
            counter.entity.id(),
            counter.entity.version()
        ),
        event_type,
        &CounterState {
            id: counter.entity.id(),
            value: counter.value,
        },
        &counter.entity,
    )
}
