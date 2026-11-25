use distributed::{sourced, Entity, Snapshot};
use serde::{Deserialize, Serialize};

// ============================================================================
// Default case: id + all fields (the shared Todo)
// ============================================================================

pub use common::todo::{Todo, TodoSnapshot};

// ============================================================================
// Custom ID key: snapshot(id = "sku")
// ============================================================================

#[derive(Default, Snapshot)]
#[snapshot(id = "sku")]
pub struct Inventory {
    pub entity: Entity,
    pub sku: String,
    pub available: u32,
}

#[sourced(entity)]
impl Inventory {
    pub fn new() -> Self {
        Self::default()
    }

    #[event("initialized")]
    pub fn create(&mut self, id: String, sku: String, available: u32) {
        self.entity.set_id(&id);
        self.sku = sku;
        self.available = available;
    }

    #[event("restocked")]
    pub fn restock(&mut self, qty: u32) {
        self.available += qty;
    }
}

// ============================================================================
// serde(skip) field exclusion
// ============================================================================

#[derive(Default, Serialize, Deserialize, Snapshot)]
pub struct Order {
    pub entity: Entity,
    pub customer: String,
    pub total: u64,
    #[serde(skip)]
    pub cached_label: String,
}

#[sourced(entity)]
impl Order {
    pub fn new() -> Self {
        Self::default()
    }

    #[event("placed")]
    pub fn place(&mut self, id: String, customer: String, total: u64) {
        self.entity.set_id(&id);
        self.customer = customer;
        self.total = total;
        self.cached_label = format!("Order for {}", self.customer);
    }
}

// ============================================================================
// Works with #[sourced(entity)] on impl
// ============================================================================

#[derive(Default, Snapshot)]
pub struct Counter {
    pub entity: Entity,
    pub count: i64,
}

#[distributed::sourced(entity)]
impl Counter {
    pub fn new() -> Self {
        Self::default()
    }

    #[event("initialized")]
    pub fn initialize(&mut self, id: String) {
        self.entity.set_id(&id);
    }

    #[event("incremented")]
    pub fn increment(&mut self, amount: i64) {
        self.count += amount;
    }
}

// ============================================================================
// Custom entity field name and snapshot version
// ============================================================================

#[derive(Default, Snapshot)]
#[snapshot(entity = "my_entity", version = 3)]
pub struct Widget {
    pub my_entity: Entity,
    pub name: String,
    pub weight: f64,
}

#[sourced(my_entity)]
impl Widget {
    pub fn new() -> Self {
        Self::default()
    }

    #[event("initialized")]
    pub fn create(&mut self, id: String, name: String, weight: f64) {
        self.my_entity.set_id(&id);
        self.name = name;
        self.weight = weight;
    }
}

// ============================================================================
// serde(skip, default) field exclusion (EntityEmitter pattern)
// ============================================================================

#[derive(Default, Serialize, Deserialize)]
pub struct DummyEmitter;

#[allow(dead_code)]
#[derive(Default, Serialize, Deserialize, Snapshot)]
pub struct Notifier {
    pub entity: Entity,
    pub message: String,
    #[serde(skip, default)]
    pub emitter: DummyEmitter,
}

#[sourced(entity)]
impl Notifier {
    pub fn new() -> Self {
        Self::default()
    }

    #[event("sent")]
    pub fn send(&mut self, id: String, message: String) {
        self.entity.set_id(&id);
        self.message = message;
    }
}
