use distributed::command;
use distributed::command::Eventual;
use serde::Deserialize;

use super::TodoStatusPayload;
use crate::{domain_commands, Todo};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoReopenInput {
    pub todo_id: String,
}

pub type TodoReopenPayload = TodoStatusPayload;

command! {
    name: "todo.reopen",
    transition: domain_commands::Reopen,
    aggregate: Todo,
    input: TodoReopenInput,
    outcome: Eventual<TodoReopenPayload>,
    shard: |input| input.todo_id.clone(),
    load: required,
    roles: ["user", "admin"],
    field: "todos_reopen",
    invoke: |todo, _input, principal| todo.reopen(principal),
    payload: |todo, _principal| TodoStatusPayload::from_todo(&**todo),
}
