use distributed::command;
use distributed::command::Eventual;
use serde::{Deserialize, Serialize};

use super::support::admin_user;
use crate::{domain_commands, Todo, TodoState};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoForceArchiveInput {
    pub todo_id: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct TodoForceArchivePayload {
    pub todo_id: String,
    pub owner_id: String,
    pub status: String,
    pub archived_by: String,
}

command! {
    name: "todo.force_archive",
    transition: domain_commands::ForceArchive,
    aggregate: Todo,
    input: TodoForceArchiveInput,
    outcome: Eventual<TodoForceArchivePayload>,
    shard: |input| input.todo_id.clone(),
    roles: ["admin"],
    field: "todos_force_archive",
    load: required,
    guard: admin_user,
    invoke: |todo, _input, _principal| todo.force_archive(),
    payload: |todo, principal| {
        let state = TodoState::from(&**todo);
        TodoForceArchivePayload {
            todo_id: state.todo_id,
            owner_id: state.owner_id,
            status: state.status,
            archived_by: principal.to_string(),
        }
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_todo_id() {
        let input = TodoForceArchiveInput {
            todo_id: "todo-1".into(),
        };
        assert_eq!(ForceArchive::shard(&input), "todo-1");
    }
}
