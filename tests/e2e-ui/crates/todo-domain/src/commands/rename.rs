use distributed::command::Eventual;
use distributed::portable_command;
use serde::{Deserialize, Serialize};

use crate::{domain_commands, Todo, TodoState};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoRenameInput {
    pub todo_id: String,
    pub title: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct TodoRenamePayload {
    pub todo_id: String,
    pub title: String,
    pub status: String,
}

portable_command! {
    name: "todo.rename",
    transition: domain_commands::Rename,
    aggregate: Todo,
    input: TodoRenameInput,
    outcome: Eventual<TodoRenamePayload>,
    shard: |input| input.todo_id.clone(),
    load: required,
    roles: ["user", "admin"],
    field: "todos_rename",
    invoke: |todo, input, principal| todo.rename(principal, &input.title),
    payload: |todo| {
        let state = TodoState::from(&**todo);
        TodoRenamePayload {
            todo_id: state.todo_id,
            title: state.title,
            status: state.status,
        }
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_todo_id() {
        let input = TodoRenameInput {
            todo_id: "todo-1".into(),
            title: "renamed".into(),
        };
        assert_eq!(Rename::shard(&input), "todo-1");
    }
}
