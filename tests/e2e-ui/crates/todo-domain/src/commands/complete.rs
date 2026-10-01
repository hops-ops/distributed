use distributed::command::Eventual;
use distributed::portable_command;
use serde::{Deserialize, Serialize};

use crate::{domain_commands, Todo, TodoState};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoCompleteInput {
    pub todo_id: String,
}

/// Shared complete / archive / reopen payload.
#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct TodoStatusPayload {
    pub todo_id: String,
    pub status: String,
}

impl TodoStatusPayload {
    pub(super) fn from_todo(todo: &Todo) -> Self {
        let state = TodoState::from(todo);
        Self {
            todo_id: state.todo_id,
            status: state.status,
        }
    }
}

portable_command! {
    name: "todo.complete",
    transition: domain_commands::Complete,
    aggregate: Todo,
    input: TodoCompleteInput,
    outcome: Eventual<TodoStatusPayload>,
    shard: |input| input.todo_id.clone(),
    load: required,
    roles: ["user", "admin"],
    field: "todos_complete",
    invoke: |todo, _input, principal| todo.complete(principal),
    payload: |todo| TodoStatusPayload::from_todo(&**todo),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_todo_id() {
        let input = TodoCompleteInput {
            todo_id: "todo-1".into(),
        };
        assert_eq!(Complete::shard(&input), "todo-1");
    }
}
