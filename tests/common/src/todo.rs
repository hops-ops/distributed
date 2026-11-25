//! The shared Todo aggregate.

use distributed::{sourced, Entity, Snapshot};
use serde::{Deserialize, Serialize};

#[derive(Default, Snapshot)]
pub struct Todo {
    pub entity: Entity,
    pub user_id: String,
    pub task: String,
    pub completed: bool,
}

#[sourced(entity)]
impl Todo {
    pub fn new() -> Self {
        Self::default()
    }

    #[event("initialized")]
    pub fn initialize(&mut self, id: String, user_id: String, task: String) {
        self.entity.set_id(&id);
        self.user_id = user_id;
        self.task = task;
    }

    #[event("completed", when = !self.completed)]
    pub fn complete(&mut self) {
        self.completed = true;
    }

    /// The Todo's state as a plain value (a non-event method, which the
    /// `#[sourced]` macro passes through unchanged).
    pub fn state(&self) -> TodoState {
        TodoState {
            id: self.entity.id().to_string(),
            user_id: self.user_id.clone(),
            task: self.task.clone(),
            completed: self.completed,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// What [`Todo::state`] returns; the generated `TodoSnapshot` is the
/// snapshot payload.
pub struct TodoState {
    pub id: String,
    pub user_id: String,
    pub task: String,
    pub completed: bool,
}
