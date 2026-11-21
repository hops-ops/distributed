//! Test domain: a simple Counter aggregate for microsvc tests.

use distributed::{sourced, Entity, Snapshot};
use serde::{Deserialize, Serialize};

/// A simple counter aggregate for testing microsvc dispatch.
#[derive(Default, Snapshot)]
pub struct Counter {
    pub entity: Entity,
    pub value: i64,
}

#[sourced(entity)]
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

    #[event("decremented", when = self.value >= amount)]
    pub fn decrement(&mut self, amount: i64) {
        self.value -= amount;
    }
}

/// Command payload: create a new counter.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct CreateCounter {
    pub id: String,
}

/// Command payload: increment a counter.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct IncrementCounter {
    pub id: String,
    pub amount: i64,
}

/// Command payload: decrement a counter.
#[derive(Debug, Serialize, Deserialize, distributed::CommandInput)]
pub struct DecrementCounter {
    pub id: String,
    pub amount: i64,
}
