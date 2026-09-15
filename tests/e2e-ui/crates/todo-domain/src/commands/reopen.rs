use distributed::command::Eventual;
use distributed::portable_command;
use serde::Deserialize;

use super::TodoStatusPayload;
use crate::{domain_commands, Todo};

#[derive(Debug, Deserialize, distributed::CommandInput)]
pub struct TodoReopenInput {
    pub todo_id: String,
}

pub type TodoReopenPayload = TodoStatusPayload;

portable_command! {
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
    payload: |todo| TodoStatusPayload::from_todo(&**todo),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_todo_id() {
        let input = TodoReopenInput {
            todo_id: "todo-1".into(),
        };
        assert_eq!(Reopen::shard(&input), "todo-1");
    }
}
