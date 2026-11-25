use distributed::command;
use distributed::command::Eventual;
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

command! {
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
    payload: |todo, _principal| {
        let state = TodoState::from(&**todo);
        TodoRenamePayload {
            todo_id: state.todo_id,
            title: state.title,
            status: state.status,
        }
    },
}
