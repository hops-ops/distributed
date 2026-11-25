//! Todo: mutations + projections.

use distributed::mutation_file;
use distributed::projection::lower::{EventualOnly, ProjectionDescriptor};
use distributed::Mutation;
use e2e_readmodels::Todos;
use todo_domain::{
    TodoArchivedDomainEvent, TodoCompletedDomainEvent, TodoCreatedDomainEvent,
    TodoForceArchivedDomainEvent, TodoPurgedDomainEvent, TodoReassignedDomainEvent,
    TodoRenamedDomainEvent, TodoReopenedDomainEvent,
};

/// Mutation: complete-row upsert for Todo state transfer.
///
/// Authored as GraphQL-looking syntax-only IR (not a public GraphQL field).
/// The constructor name matches the document operation: `mutation SaveTodo`.
#[allow(non_snake_case)]
pub fn SaveTodo() -> Mutation<()> {
    mutation_file!("src/mutations/save_todo.mutation.graphql")
}

/// Mutation: delete Todo by primary key (purge).
///
/// The constructor name matches the document operation: `mutation DeleteTodo`.
#[allow(non_snake_case)]
pub fn DeleteTodo() -> Mutation<()> {
    mutation_file!("src/mutations/delete_todo.mutation.graphql")
}

// Lifecycle state events → [`SaveTodo`]; purge → [`DeleteTodo`].
// Macro is `projection!` (crate root); `distributed::projection` is the module.
distributed::projection! {
    pub const TODOS: ProjectionDescriptor<EventualOnly> = {
        name: "project_todos",
        version: 1,
        epoch: "e2e-ui-todos-v2",
        model: Todos,
        on {
            events: [
                TodoCreatedDomainEvent,
                TodoRenamedDomainEvent,
                TodoCompletedDomainEvent,
                TodoReopenedDomainEvent,
                TodoReassignedDomainEvent,
                TodoArchivedDomainEvent,
                TodoForceArchivedDomainEvent,
            ],
            mutation: SaveTodo,
            input: { todo: body },
        },
        on {
            events: [TodoPurgedDomainEvent],
            mutation: DeleteTodo,
            input: { todo_id: aggregate_id },
        },
    };
}

#[cfg(test)]
mod tests {
    use distributed::domain_event::DomainEventContract;
    use distributed::projection::placement::ProjectionExecutionClass;
    use distributed::{DomainEventBodyKind, MutationKind, RelationalReadModel, RowValue};

    use super::*;
    use todo_domain::{Todo, TodoPurgedDomainEvent};

    #[test]
    fn complete_lowers_to_full_row_upsert() {
        let mut todo = Todo::default();
        todo.create("todo-1", "alice", "Read").unwrap();
        todo.reassign("alice", "bob").unwrap();
        todo.complete("alice").unwrap();
        let occurrence = todo.entity.pending_domain_events().last().unwrap();
        let lowered = TODOS.server_executor().unwrap().plan(occurrence).unwrap();
        let [(resolved, record)] = lowered.records().collect::<Vec<_>>()[..] else {
            panic!("expected one record");
        };
        assert_eq!(resolved.kind(), MutationKind::Upsert);
        assert_eq!(
            record.values().get("status"),
            Some(&RowValue::String("completed".into()))
        );
        assert_eq!(Todos::schema().table_name, "todos");
        assert_eq!(
            TODOS.eventual().execution_class(),
            ProjectionExecutionClass::Causal
        );
    }

    #[test]
    fn purge_lowers_to_delete() {
        let mut todo = Todo::default();
        todo.create("todo-1", "alice", "Read").unwrap();
        todo.purge("alice").unwrap();
        let occurrence = todo.entity.pending_domain_events().last().unwrap();
        assert_eq!(
            occurrence.descriptor().body.kind,
            DomainEventBodyKind::Deletion
        );
        assert_eq!(
            occurrence.descriptor(),
            &TodoPurgedDomainEvent::descriptor()
        );
        let lowered = TODOS.server_executor().unwrap().plan(occurrence).unwrap();
        let [(resolved, record)] = lowered.records().collect::<Vec<_>>()[..] else {
            panic!("expected one record");
        };
        assert_eq!(resolved.kind(), MutationKind::Delete);
        assert_eq!(
            record.key().get("todo_id"),
            Some(&RowValue::String("todo-1".into()))
        );
    }

    #[test]
    fn mutations_are_event_free() {
        let json = serde_json::to_value(SaveTodo().program().unwrap())
            .unwrap()
            .to_string();
        assert!(!json.contains("event_name"));
    }
}
