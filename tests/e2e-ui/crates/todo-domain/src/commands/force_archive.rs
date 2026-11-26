use distributed::command;
use distributed::command::Eventual;
use serde::{Deserialize, Serialize};

use crate::{domain_commands, Todo, TodoState};
use distributed::microsvc::guards;

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct TodoForceArchiveInput {
    pub todo_id: String,
}

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
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
    guard: guards::role("admin"),
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
