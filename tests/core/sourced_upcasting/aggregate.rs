use distributed::{sourced, Entity};

pub type InitializedV1 = (String, String, String);
pub type InitializedV2 = (String, String, String, u8);
pub type InitializedV3 = (String, String, String, u8, String);

// =============================================================================
// V1 aggregate: original schema
// =============================================================================

#[derive(Default)]
pub struct TodoV1 {
    pub entity: Entity,
    pub user_id: String,
    pub task: String,
    pub completed: bool,
}

#[sourced(entity)]
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

#[sourced(entity, upcasters(
    ("initialized", 1 => 2, InitializedV1 => InitializedV2, Self::upcast_initialized_v1_v2),
))]
impl TodoV2 {
    pub fn upcast_initialized_v1_v2(payload: InitializedV1) -> InitializedV2 {
        upcast_initialized_v1_v2(payload)
    }

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

#[sourced(entity, upcasters(
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
