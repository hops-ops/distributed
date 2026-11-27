use distributed::command;
use distributed::command::Eventual;
use distributed::command_input_defaults;
use serde::{Deserialize, Serialize};

use crate::{domain_commands, Todo, TodoState};
use distributed::microsvc::guards;

#[derive(Debug, Deserialize, distributed::command::CommandInput)]
pub struct TodoCreateInput {
    pub todo_id: String,
    pub title: String,
}

#[derive(Debug, Serialize, distributed::command::CommandOutput)]
pub struct TodoCreatePayload {
    pub todo_id: String,
    pub owner_id: String,
    pub title: String,
    pub status: String,
}

command! {
    name: "todo.create",
    transition: domain_commands::Create,
    aggregate: Todo,
    input: TodoCreateInput,
    outcome: Eventual<TodoCreatePayload>,
    shard: |input| input.todo_id.clone(),
    roles: ["user", "admin"],
    field: "todos_create",
    load: create,
    guard: guards::authenticated,
    invoke: |todo, input, principal| todo.create(&input.todo_id, principal, &input.title),
    payload: |todo, _principal| {
        let state = TodoState::from(&**todo);
        TodoCreatePayload {
            todo_id: state.todo_id,
            owner_id: state.owner_id,
            title: state.title,
            status: state.status,
        }
    },
    defaults: command_input_defaults! {
        input: TodoCreateInput;
        default input.todo_id = uuid_v7();
    },
}
