use distributed::command::Eventual;
use distributed::portable_command;
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

portable_command! {
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
    payload: |todo| TodoPurgePayload {
        todo_id: todo.todo_id.clone(),
        purged: true,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_is_todo_id() {
        let input = TodoPurgeInput {
            todo_id: "todo-1".into(),
        };
        assert_eq!(Purge::shard(&input), "todo-1");
    }
}
