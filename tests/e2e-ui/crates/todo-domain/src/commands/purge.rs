use distributed::command;
use distributed::command::Eventual;
use serde::{Deserialize, Serialize};

use crate::{domain_commands, Todo};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoPurgeInput {
    pub todo_id: String,
}

#[derive(Debug, Serialize, distributed::CommandOutput)]
pub struct TodoPurgePayload {
    pub todo_id: String,
    pub purged: bool,
}

command! {
    name: "todo.purge",
    transition: domain_commands::Purge,
    aggregate: Todo,
    input: TodoPurgeInput,
    outcome: Eventual<TodoPurgePayload>,
    shard: |input| input.todo_id.clone(),
    load: required,
    roles: ["user", "admin"],
    field: "todos_purge",
    invoke: |todo, _input, principal| todo.purge(principal),
    payload: |todo, _principal| TodoPurgePayload {
        todo_id: todo.todo_id.clone(),
        purged: true,
    },
}
