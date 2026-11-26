use distributed::{sourced, Entity};

struct Workflow {
    entity: Entity,
}

#[sourced(entity, aggregate_type = "workflow")]
impl Workflow {
    #[event("user.completed")]
    pub fn user_completed(&mut self) {}

    // Distinct event name, but only the last `.`-segment names the enum
    // variant, so both events derive `Completed` — rejected up front instead
    // of emitting an enum with duplicate variants.
    #[event("admin.completed")]
    pub fn admin_completed(&mut self) {}
}

fn main() {}
