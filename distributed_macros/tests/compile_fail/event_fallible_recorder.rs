use distributed::{sourced, Entity};

#[derive(Default)]
struct Todo {
    entity: Entity,
    title: String,
}

#[sourced(entity)]
impl Todo {
    // The event is recorded before this body runs: an `Err` here would leave
    // a stored event whose state change never happened.
    #[event("todo.renamed")]
    fn rename(&mut self, title: String) -> Result<(), String> {
        if title.is_empty() {
            return Err("empty".into());
        }
        self.title = title;
        Ok(())
    }
}

fn main() {}
