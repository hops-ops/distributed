use distributed::{sourced, Entity};

#[derive(Default)]
pub struct Todo {
    entity: Entity,
    completed: bool,
}

#[sourced(entity, aggregate_type = "todo")]
impl Todo {
    pub fn complete(&mut self) -> distributed::SourcedResult {
        self.record_completed()?;
        Ok(())
    }

    // Records on another receiver and through a nested item's `self`: neither
    // is this aggregate's transition, so no `domain_commands::Unrelated`.
    pub fn unrelated(&mut self, other: &mut Todo) {
        other.record_completed().unwrap();
        struct Other;
        impl Other {
            #[allow(dead_code)]
            fn record_completed(&mut self) {}
            #[allow(dead_code)]
            fn action(&mut self) {
                self.record_completed();
            }
        }
    }

    #[event("todo.completed", domain = event)]
    fn record_completed(&mut self) {
        self.completed = true;
    }
}

fn main() {
    let _: Option<domain_commands::Complete> = None;
    let _: Option<domain_commands::Unrelated> = None;
}
