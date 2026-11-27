use distributed::{sourced, Entity, Snapshottable};
use serde::{Deserialize, Serialize};

pub type InitializedV1 = (String, String, String);
pub type InitializedV2 = (String, String, String, u8);
pub type InitializedV3 = (String, String, String, u8, String);

// =============================================================================
// V1 aggregate: original schema
// =============================================================================

/// A simple Todo at v1 — no priority field.
#[derive(Default)]
pub struct TodoV1 {
    pub entity: Entity,
    pub user_id: String,
    pub task: String,
    pub completed: bool,
}

#[sourced(entity, aggregate_type = "Todo")]
impl TodoV1 {
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
}

// =============================================================================
// V2 aggregate: added priority field + upcaster
// =============================================================================

/// Upcasts Initialized v1 (id, user_id, task) → v2 (id, user_id, task, priority)
pub fn upcast_initialized_v1_v2((id, user_id, task): InitializedV1) -> InitializedV2 {
    (id, user_id, task, 0)
}

#[derive(Default)]
pub struct TodoV2 {
    pub entity: Entity,
    pub user_id: String,
    pub task: String,
    pub priority: u8,
    pub completed: bool,
}

#[sourced(entity, aggregate_type = "Todo", upcasters(
    ("initialized", 1 => 2, InitializedV1 => InitializedV2, upcast_initialized_v1_v2),
))]
impl TodoV2 {
    #[event("initialized", version = 2)]
    pub fn initialize(&mut self, id: String, user_id: String, task: String, priority: u8) {
        self.entity.set_id(&id);
        self.user_id = user_id;
        self.task = task;
        self.priority = priority;
    }

    #[event("completed", when = !self.completed)]
    pub fn complete(&mut self) {
        self.completed = true;
    }
}

// =============================================================================
// V3 aggregate: added due_date field + chained upcasters
// =============================================================================

/// Upcasts Initialized v2 (id, user_id, task, priority) → v3 (id, user_id, task, priority, due_date)
pub fn upcast_initialized_v2_v3((id, user_id, task, priority): InitializedV2) -> InitializedV3 {
    (id, user_id, task, priority, String::new())
}

#[derive(Default)]
pub struct TodoV3 {
    pub entity: Entity,
    pub user_id: String,
    pub task: String,
    pub priority: u8,
    pub due_date: String,
    pub completed: bool,
}

#[sourced(entity, aggregate_type = "Todo", upcasters(
    ("initialized", 1 => 2, InitializedV1 => InitializedV2, upcast_initialized_v1_v2),
    ("initialized", 2 => 3, InitializedV2 => InitializedV3, upcast_initialized_v2_v3),
))]
impl TodoV3 {
    #[event("initialized", version = 3)]
    pub fn initialize(
        &mut self,
        id: String,
        user_id: String,
        task: String,
        priority: u8,
        due_date: String,
    ) {
        self.entity.set_id(&id);
        self.user_id = user_id;
        self.task = task;
        self.priority = priority;
        self.due_date = due_date;
    }

    #[event("completed", when = !self.completed)]
    pub fn complete(&mut self) {
        self.completed = true;
    }
}

// =============================================================================
// Snapshottable impl for TodoV2 (needed for snapshot + upcasting tests)
// =============================================================================

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TodoV2Snapshot {
    pub id: String,
    pub user_id: String,
    pub task: String,
    pub priority: u8,
    pub completed: bool,
}

impl Snapshottable for TodoV2 {
    type Snapshot = TodoV2Snapshot;

    fn create_snapshot(&self) -> TodoV2Snapshot {
        TodoV2Snapshot {
            id: self.entity.id().to_string(),
            user_id: self.user_id.clone(),
            task: self.task.clone(),
            priority: self.priority,
            completed: self.completed,
        }
    }

    fn restore_from_snapshot(&mut self, s: TodoV2Snapshot) {
        self.entity.set_id(&s.id);
        self.user_id = s.user_id;
        self.task = s.task;
        self.priority = s.priority;
        self.completed = s.completed;
    }
}
